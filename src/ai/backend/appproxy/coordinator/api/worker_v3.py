"""
v3 polling endpoints for AppProxy workers.

These handlers implement the worker poll-based control plane described in
BA-6068. They are intentionally segregated from the legacy ``worker`` (v2)
endpoints so the two scopes can be authenticated independently — see
``auth_required("worker_v3")`` in ``.utils`` (added by A7).

Mitigations applied here:

* SEC-2 — Both ``get_routes`` and ``refresh_token`` require a token whose
  ``scope`` is exactly ``"worker_v3"``. ``auth_required("worker_v3")`` performs
  the scope check; replays of legacy ``"worker"`` tokens are rejected.
* SEC-4 — ``Cache-Control: no-store, private`` is set on every response that
  carries route snapshot or token material so intermediate caches cannot
  retain authority-bound payloads.
* RC-3 — Heartbeats update Valkey ``proxy-worker.committed_route_version``
  monotonically via ``valkey_live.cas_max``, an atomic Lua compare-and-set
  on max. A slow/restarted worker can no longer overwrite a newer committed
  value reported by a faster poll cycle; coordinator-side ``pick_worker``
  comparison reads the same key.
"""

from __future__ import annotations

import logging
import uuid as uuid_module
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

import aiohttp_cors
from aiohttp import web
from pydantic import ValidationError

from ai.backend.appproxy.common.config import get_default_redis_key_ttl
from ai.backend.appproxy.common.dto.worker_contract import (
    RefreshTokenResponse,
    SecretFetchResponse,
    WorkerHeartbeatResponse,
    WorkerRegistrationRequest,
    WorkerRegistrationResponseV3,
    WorkerSelfReport,
)
from ai.backend.appproxy.common.errors import GenericBadRequest, GenericForbidden, ObjectNotFound
from ai.backend.appproxy.common.types import (
    BackendKind,
    CORSOptions,
    WebMiddleware,
)
from ai.backend.appproxy.coordinator.models import Worker, WorkerAppFilter, WorkerStatus
from ai.backend.appproxy.coordinator.models.utils import execute_with_txn_retry
from ai.backend.appproxy.coordinator.services.poll_token import PollTokenService
from ai.backend.appproxy.coordinator.services.route_snapshot import RouteSnapshotService
from ai.backend.appproxy.coordinator.services.secret_resolver import (
    SecretRefParseError,
    SecretResolver,
)
from ai.backend.appproxy.coordinator.types import RootContext
from ai.backend.logging import BraceStyleAdapter

from .utils import auth_required, worker_v3_auth_required

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession as SASession

SnapshotFormat = Literal["unified", "traefik-native", "continuum-native"]
_VALID_FORMATS: frozenset[str] = frozenset(("unified", "traefik-native", "continuum-native"))

log = BraceStyleAdapter(logging.getLogger(__spec__.name))


# ``poll_interval_hint_ms`` is advisory — workers may back off further on
# their own. 5s mirrors the default cadence used by the SELF_HOSTED workers
# in benchmarks.
_DEFAULT_POLL_INTERVAL_HINT_MS = 5000

_HEALTH_COMPONENT = "backend-ai-appproxy-coordinator"


def _no_store_headers() -> dict[str, str]:
    """Headers applied to every v3 response carrying authority-bound data.

    SEC-4: prevents intermediate caches from retaining route snapshots or
    refreshed tokens. ``private`` is redundant when ``no-store`` is set, but
    keeping both makes the intent explicit for older proxies that honour the
    weaker directive.
    """

    return {"Cache-Control": "no-store, private"}


@worker_v3_auth_required
async def get_routes(request: web.Request) -> web.StreamResponse:
    """``GET /api/v3/worker/{worker_id}/routes``.

    Returns a route snapshot (200) or an empty 304 when the worker is already
    up-to-date as determined by the ``If-None-Match`` etag and/or the
    ``since`` route_version cursor.

    Response always carries:
      * ``Cache-Control: no-store, private`` (SEC-4)
      * ``ETag`` — current snapshot fingerprint
      * ``X-Route-Version`` — monotonically increasing route_version
    """

    root_ctx: RootContext = request.app["_root.context"]
    worker_id = UUID(request.match_info["worker_id"])

    fmt_raw = request.query.get("format", "unified")
    if fmt_raw not in _VALID_FORMATS:
        raise web.HTTPBadRequest(
            reason=f"Unsupported format: {fmt_raw}",
        )
    fmt: SnapshotFormat = cast(SnapshotFormat, fmt_raw)
    since_raw = request.query.get("since")
    since: int | None
    try:
        since = int(since_raw) if since_raw is not None else None
    except ValueError:
        since = None
    if_none_match = request.headers.get("If-None-Match")

    service = RouteSnapshotService(root_ctx.db, root_ctx.valkey_live)
    snapshot = await service.get_snapshot(
        worker_id=worker_id,
        format=fmt,
        since=since,
        etag_hint=if_none_match,
    )

    headers: dict[str, str] = _no_store_headers()
    headers["ETag"] = snapshot.etag
    headers["X-Route-Version"] = str(snapshot.route_version)

    if snapshot.status_code == 304:
        return web.Response(status=304, headers=headers)

    return web.json_response(
        snapshot.body,
        headers=headers,
    )


@worker_v3_auth_required
async def refresh_token(request: web.Request) -> web.StreamResponse:
    """``POST /api/v3/worker/{worker_id}/refresh-token``.

    Bumps ``workers.poll_token_revision`` (invalidating every outstanding
    token for this worker) and returns a freshly-issued token bound to the
    new revision.

    RC-4 mitigation: a per-worker Valkey lock guarantees single-flight
    refresh. If two callers race (e.g. PollTokenRefresher + Continuum's
    AsyncAuthStrategy both noticed expiry), the loser does NOT bump the
    revision again — instead it re-reads the freshly-bumped row and issues
    a token bound to that revision, keeping caller-visible behaviour
    idempotent.
    """

    root_ctx: RootContext = request.app["_root.context"]
    worker_id = UUID(request.match_info["worker_id"])

    token_service = PollTokenService(
        jwt_secret=root_ctx.local_config.secrets.poll_token_secret,
    )

    # RC-4 mitigation: single-flight refresh.
    lock_acquired = await token_service.acquire_refresh_lock(worker_id, root_ctx.valkey_live)
    if not lock_acquired:
        # Another caller is already refreshing — return the current valid
        # token by re-fetching the worker row instead of bumping again.
        async with root_ctx.db.begin_readonly_session() as sess:
            worker = await Worker.get(sess, worker_id)
            current_revision = worker.poll_token_revision
        # Issue a token bound to the SAME revision (idempotent caller view).
        token_str, expires_at = token_service.issue(
            worker_id=worker_id,
            scope={},
            token_revision=current_revision,
        )
    else:

        async def _bump(sess: SASession) -> int:
            worker = await Worker.get(sess, worker_id)
            worker.poll_token_revision = (worker.poll_token_revision or 0) + 1
            return worker.poll_token_revision

        async with root_ctx.db.connect() as db_conn:
            new_revision = await execute_with_txn_retry(_bump, root_ctx.db.begin_session, db_conn)

        # ``scope`` is currently empty on refresh — coordinator-owned policy
        # will populate it in a follow-up slice; until then we faithfully
        # roundtrip the issued shape (sub/aud are baked into the service).
        token_str, expires_at = token_service.issue(
            worker_id=worker_id,
            scope={},
            token_revision=new_revision,
        )

    return web.json_response(
        RefreshTokenResponse(token=token_str, expires_at=expires_at).model_dump(mode="json"),
        headers=_no_store_headers(),
    )


@worker_v3_auth_required
async def heartbeat_v3(request: web.Request) -> web.StreamResponse:
    """``PATCH /api/v3/worker/{worker_id}/heartbeat``.

    Workers report their liveness here. v3-polling workers (Continuum)
    attach a :class:`WorkerSelfReport` body with the route version they
    just applied; legacy NATIVE / TRAEFIK workers that do not poll send
    an empty body and the coordinator treats every field as zero. Either
    way the handler refreshes ``last_polled_at`` on the row and the
    ``proxy-worker.last_seen`` Valkey hash that ``check_worker_lost``
    consults.
    """

    root_ctx: RootContext = request.app["_root.context"]
    worker_id = UUID(request.match_info["worker_id"])

    report: WorkerSelfReport | None
    if request.content_length:
        try:
            body = await request.json()
        except Exception as e:
            raise GenericBadRequest(f"Invalid JSON body: {e}") from e
        try:
            report = WorkerSelfReport.model_validate(body)
        except ValidationError as e:
            raise GenericBadRequest(f"Invalid WorkerSelfReport: {e}") from e
    else:
        report = None

    now = datetime.now(UTC)
    applied_route_version = int(report.applied_route_version) if report else 0

    async def _update(sess: SASession) -> str:
        worker = await Worker.get(sess, worker_id)
        worker.last_polled_at = now
        # OPS-2 mitigation: a v3-polling worker stays in STARTING until
        # the data plane actually applies its first snapshot. Promote
        # STARTING → ALIVE inside the same transaction as
        # ``last_polled_at`` so the cold-start gate flips atomically;
        # ALIVE workers are eligible for pick_worker assignment from the
        # next request onward. For workers without a polling subprocess
        # the body is empty and we simply keep them ALIVE.
        if worker.status == WorkerStatus.STARTING and applied_route_version > 0:
            worker.status = WorkerStatus.ALIVE
            log.info(
                "Worker {} cold-start complete (applied_route_version={}); STARTING -> ALIVE",
                worker_id,
                applied_route_version,
            )
        elif worker.status != WorkerStatus.ALIVE and applied_route_version == 0:
            # Non-polling NATIVE / TRAEFIK workers — empty body, no
            # cold-start gate. Flip to ALIVE if they were marked LOST
            # by an earlier missed heartbeat.
            worker.status = WorkerStatus.ALIVE
        return worker.authority

    async with root_ctx.db.connect() as db_conn:
        authority = await execute_with_txn_retry(_update, root_ctx.db.begin_session, db_conn)

    # check_worker_lost still scans the ``proxy-worker.last_seen`` Valkey
    # hash. Update it for every heartbeat — polling and non-polling alike.
    ttl = get_default_redis_key_ttl()
    await root_ctx.valkey_live.hset_with_expiry(
        "proxy-worker.last_seen",
        {authority: str(now.timestamp())},
        ttl,
    )

    # RC-3: cas_max is atomic compare-and-set on max — slow heartbeats can no
    # longer overwrite a newer committed value. Only relevant for polling
    # workers; non-polling workers contribute 0 which the CAS ignores.
    if applied_route_version > 0:
        key = f"proxy-worker.committed_route_version:{worker_id}"
        await root_ctx.valkey_live.cas_max(key, applied_route_version, ex=60)

    request["do_not_print_access_log"] = True
    return web.json_response(
        WorkerHeartbeatResponse(
            ack_at=now,
            poll_interval_hint_ms=_DEFAULT_POLL_INTERVAL_HINT_MS,
        ).model_dump(mode="json"),
        headers=_no_store_headers(),
    )


@worker_v3_auth_required
async def deregister_v3(request: web.Request) -> web.StreamResponse:
    """``DELETE /api/v3/worker/{worker_id}``.

    Clean shutdown unregister. Decrements ``nodes`` and marks the worker
    LOST when the last node deregisters so ``pick_worker`` stops handing
    it new circuits.
    """

    root_ctx: RootContext = request.app["_root.context"]
    worker_id = UUID(request.match_info["worker_id"])

    async def _update(sess: SASession) -> None:
        worker = await Worker.get(sess, worker_id)
        worker.nodes -= 1
        if worker.nodes <= 0:
            worker.status = WorkerStatus.LOST

    async with root_ctx.db.connect() as db_conn:
        await execute_with_txn_retry(_update, root_ctx.db.begin_session, db_conn)

    return web.json_response({"success": True}, headers=_no_store_headers())


@worker_v3_auth_required
async def get_secret(request: web.Request) -> web.StreamResponse:
    """``GET /api/v3/worker/{worker_id}/secrets/{ref}``.

    OB-COMP-1 mitigation (option (b) — opaque ref + dedicated resolution):
    Resolves the opaque ``secret://worker/{worker_id}/{kind}[/...]`` reference
    embedded in a route snapshot into the raw secret material the worker
    needs immediately before issuing the upstream request.

    The ``ref`` path parameter is URL-encoded by the caller; the resolver
    URL-decodes it once. SEC-2 cross-worker resolution is enforced inside
    :class:`SecretResolver` — even if a worker A token somehow obtained a
    reference bound to worker B, resolution stops before any secret is read.

    Response contract:
      * 200 + ``{"secret": "<raw>"}``           — known kind, value available.
      * 400 + ``GenericBadRequest`` body        — malformed reference URI.
      * 403 + ``GenericForbidden`` body         — cross-worker resolution.
      * 404 + ``ObjectNotFound`` body           — unknown kind or stubbed
                                                  (e.g. inference token not
                                                  yet stored in DB).

    Caching / leakage defences:
      * ``Cache-Control: no-store, private`` (SEC-4) so intermediates never
        retain the body.
      * The local ``secret`` variable is dropped from the handler scope as
        soon as the response is constructed; the JSON encoder owns the
        only remaining reference, which is freed when the response is
        sent. Combined with ``no-store`` this keeps the secret out of
        worker disk caches and access logs (``do_not_print_access_log``
        suppresses the path/query from request logs, but the path itself
        only carries the opaque ref — never the secret value).
    """

    root_ctx: RootContext = request.app["_root.context"]
    worker_id = UUID(request.match_info["worker_id"])
    raw_ref = request.match_info["ref"]

    resolver = SecretResolver(root_ctx)
    try:
        secret = await resolver.resolve(worker_id, raw_ref)
    except SecretRefParseError as exc:
        raise GenericBadRequest(f"Malformed secret reference: {exc}") from exc
    except PermissionError as exc:
        raise GenericForbidden(str(exc)) from exc

    if secret is None:
        raise ObjectNotFound(object_name="secret")

    headers = _no_store_headers()
    # Suppress access log: the URL path embeds the opaque ref, which is
    # safe to log on its own (it carries no secret material), but skipping
    # the access log line keeps the resolution endpoint free of correlation
    # signal that could help an attacker map refs to upstream backends.
    request["do_not_print_access_log"] = True

    response = web.json_response(
        SecretFetchResponse(secret=secret).model_dump(mode="json"),
        headers=headers,
    )
    # Drop the local strong reference immediately; the response writer
    # owns the only remaining handle through the JSON-encoded body, which
    # is released once the response is sent. This is a defence-in-depth
    # complement to ``Cache-Control: no-store, private`` — it shortens
    # the in-process lifetime of the secret string to the duration of a
    # single response send.
    del secret
    return response


@auth_required("worker")
async def register_v3(request: web.Request) -> web.StreamResponse:
    """``POST /api/v3/worker``.

    v3 worker registration with bootstrap auth (``X-BackendAI-Token:
    <api_secret>``). The response includes a freshly-issued ``poll_token``
    so the caller can switch to Bearer auth on every subsequent v3 call —
    no separate refresh round trip is required for first use.

    Request body is :class:`WorkerRegistrationRequest` — the SAME wire
    contract spoken by the legacy v2 ``PUT /api/worker`` endpoint. The
    two endpoints differ only in the response shape: v2 returns a full
    ``WorkerResponseModel`` (worker row + slots) so older Python workers
    can refresh their cached slot list, while v3 returns the bootstrap
    ``poll_token`` plus minimal identity (:class:`WorkerRegistrationResponseV3`)
    so the caller can switch to Bearer auth immediately.
    """

    root_ctx: RootContext = request.app["_root.context"]
    try:
        body = await request.json()
    except Exception as e:
        raise GenericBadRequest(f"Invalid JSON body: {e}") from e

    try:
        params = WorkerRegistrationRequest.model_validate(body)
    except ValidationError as e:
        raise GenericBadRequest(f"Invalid WorkerRegistrationRequest: {e}") from e

    # Apply v3-specific defaults — Continuum callers may omit
    # mode/backend_kind, in which case we infer external-backend +
    # continuum from the endpoint default. ``WorkerMode`` (self-hosted vs
    # external-backend) is derived from ``backend_kind`` via
    # :attr:`BackendKind.worker_mode` and is not stored as a separate column.
    backend_kind = params.backend_kind or BackendKind.CONTINUUM

    capabilities = params.capabilities or {}
    scope = params.scope or {}

    # OPS-2: workers that declare ``supports_v3_polling`` start in
    # STARTING and transition to ALIVE on the first heartbeat carrying
    # ``applied_route_version > 0``.
    supports_v3_polling = bool(capabilities.get("supports_v3_polling"))
    initial_status = WorkerStatus.STARTING if supports_v3_polling else WorkerStatus.ALIVE
    accepted_traffics = list(params.accepted_traffics)
    port_range: tuple[int, int] | None = (
        (params.port_range[0], params.port_range[1]) if params.port_range else None
    )
    authority = params.authority

    async def _upsert(sess: SASession) -> tuple[UUID, int, dict[str, Any]]:
        try:
            worker = await Worker.find_by_authority(sess, authority)
            worker.frontend_mode = params.frontend_mode
            worker.protocol = params.protocol
            worker.hostname = params.hostname
            worker.tls_listen = params.tls_listen
            worker.tls_advertised = params.tls_advertised
            worker.api_port = params.api_port
            if port_range is not None:
                worker.port_range = port_range
            if params.wildcard_domain is not None:
                worker.wildcard_domain = params.wildcard_domain
            if params.wildcard_traffic_port is not None:
                worker.wildcard_traffic_port = params.wildcard_traffic_port
            if params.traefik_last_used_marker_path is not None:
                worker.traefik_last_used_marker_path = params.traefik_last_used_marker_path
            worker.filtered_apps_only = params.filtered_apps_only
            worker.backend_kind = backend_kind
            if capabilities:
                worker.capabilities = capabilities
            if scope:
                worker.scope = scope
            worker.updated_at = datetime.now(UTC)
            worker.nodes += 1
            worker.status = initial_status
            worker.poll_token_revision = (worker.poll_token_revision or 0) + 1
            new_revision = worker.poll_token_revision
            return worker.id, new_revision, dict(scope) if scope else {}
        except ObjectNotFound:
            worker = Worker.create(
                uuid_module.uuid4(),
                authority,
                params.frontend_mode,
                params.protocol,
                params.hostname,
                params.tls_listen,
                params.tls_advertised,
                params.api_port,
                accepted_traffics,
                port_range=port_range,
                wildcard_domain=params.wildcard_domain,
                wildcard_traffic_port=params.wildcard_traffic_port,
                filtered_apps_only=params.filtered_apps_only,
                traefik_last_used_marker_path=params.traefik_last_used_marker_path,
                status=initial_status,
                backend_kind=backend_kind,
            )
            if capabilities:
                worker.capabilities = capabilities
            if scope:
                worker.scope = scope
            worker.poll_token_revision = 1
            sess.add(worker)
            await sess.flush()
            await sess.refresh(worker)
            return worker.id, 1, dict(scope) if scope else {}

    async def _sync_app_filters(sess: SASession, worker_id: UUID) -> None:
        """Idempotently merge declared ``app_filters`` into the row.

        Mirrors the legacy ``worker_v2.update_worker`` behaviour so v3
        register is a true superset — Python Workers migrating from the
        v2 endpoint do not lose their ``filtered_apps_only`` semantics.
        Filters already present (matched by ``(name, value)``) are no-ops;
        new entries are inserted. We do NOT delete filters that disappear
        from the request — that matches v2's append-only semantics and
        keeps the change additive.
        """
        for filter_entry in params.app_filters:
            try:
                await WorkerAppFilter.find_by_rule(
                    sess, worker_id, filter_entry.key, filter_entry.value
                )
            except ObjectNotFound:
                filter_row = WorkerAppFilter.create(
                    uuid_module.uuid4(),
                    property_name=filter_entry.key,
                    property_value=filter_entry.value,
                    worker=worker_id,
                )
                sess.add(filter_row)

    async def _upsert_and_filters(
        sess: SASession,
    ) -> tuple[UUID, int, dict[str, Any]]:
        result = await _upsert(sess)
        await _sync_app_filters(sess, result[0])
        return result

    async with root_ctx.db.connect() as db_conn:
        worker_id, token_revision, scope_dict = await execute_with_txn_retry(
            _upsert_and_filters, root_ctx.db.begin_session, db_conn
        )

    token_service = PollTokenService(
        jwt_secret=root_ctx.local_config.secrets.poll_token_secret,
    )
    token_str, expires_at = token_service.issue(
        worker_id=worker_id,
        scope=scope_dict,
        token_revision=token_revision,
    )
    log.info("v3 worker registered: authority={} id={}", authority, worker_id)

    return web.json_response(
        WorkerRegistrationResponseV3(
            worker_id=worker_id,
            poll_token=token_str,
            expires_at=expires_at,
            token_revision=token_revision,
        ).model_dump(mode="json"),
        headers=_no_store_headers(),
    )


async def health(request: web.Request) -> web.StreamResponse:
    """``GET /api/v3/worker/{worker_id}/health``.

    Unauthenticated sentinel used by Continuum data-plane workers to detect
    that the coordinator is still the right control plane to follow. The
    handler intentionally avoids any DB read so it stays available during
    schema migrations.
    """

    worker_id = request.match_info["worker_id"]
    now = datetime.now(UTC)
    return web.json_response({
        "component": _HEALTH_COMPONENT,
        "worker_id": worker_id,
        "route_version": 0,
        "ts": now.isoformat(),
    })


def create_app(
    default_cors_options: CORSOptions,
) -> tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app["prefix"] = "api/v3/worker"
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    # v3 worker registration — bootstrap auth with X-BackendAI-Token, returns
    # poll_token. The v3 protocol equivalent of legacy ``PUT /api/worker``.
    root_resource = cors.add(app.router.add_resource(r""))
    cors.add(root_resource.add_route("POST", register_v3))
    cors.add(add_route("DELETE", "/{worker_id}", deregister_v3))
    cors.add(add_route("GET", "/{worker_id}/routes", get_routes))
    cors.add(add_route("POST", "/{worker_id}/refresh-token", refresh_token))
    cors.add(add_route("PATCH", "/{worker_id}/heartbeat", heartbeat_v3))
    cors.add(add_route("GET", "/{worker_id}/health", health))
    # OB-COMP-1 mitigation: secret resolution endpoint. ``ref`` is the
    # URL-encoded ``secret://worker/{worker_id}/{kind}[/...]`` opaque
    # reference returned in the route snapshot. The placeholder uses
    # ``{ref:.*}`` so the encoded slashes inside the URI survive routing.
    cors.add(add_route("GET", "/{worker_id}/secrets/{ref:.*}", get_secret))
    return app, []
