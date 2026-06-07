"""
v3 polling endpoints for AppProxy workers.

These handlers implement the worker poll-based control plane described in
BA-6068. They share the same ``auth_required("worker")`` decorator as the
legacy v2 endpoints — the bootstrap ``X-BackendAI-Token`` header (the
Coordinator's shared ``api_secret``) is used on every call, including
register / heartbeat / routes / delete.

Mitigations applied here:

* SEC-4 — ``Cache-Control: no-store, private`` is set on every response
  that carries a route snapshot so intermediate caches cannot retain
  authority-bound payloads.
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
from typing import TYPE_CHECKING
from uuid import UUID

import aiohttp_cors
from aiohttp import web
from pydantic import ValidationError

from ai.backend.appproxy.common.config import get_default_redis_key_ttl
from ai.backend.appproxy.common.dto.worker_contract import (
    WorkerHeartbeatResponse,
    WorkerRegistrationRequest,
    WorkerRegistrationResponseV3,
)
from ai.backend.appproxy.common.errors import GenericBadRequest, ObjectNotFound
from ai.backend.appproxy.common.types import (
    BackendKind,
    CORSOptions,
    WebMiddleware,
)
from ai.backend.appproxy.coordinator.models import Worker, WorkerAppFilter, WorkerStatus
from ai.backend.appproxy.coordinator.models.utils import execute_with_txn_retry
from ai.backend.appproxy.coordinator.services.route_snapshot import RouteSnapshotService
from ai.backend.appproxy.coordinator.types import RootContext
from ai.backend.logging import BraceStyleAdapter

from .utils import auth_required

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession as SASession

log = BraceStyleAdapter(logging.getLogger(__spec__.name))


# ``poll_interval_hint_ms`` is advisory — workers may back off further on
# their own. 5s mirrors the default cadence used by the SELF_HOSTED workers
# in benchmarks.
_DEFAULT_POLL_INTERVAL_HINT_MS = 5000

_HEALTH_COMPONENT = "backend-ai-appproxy-coordinator"


def _no_store_headers() -> dict[str, str]:
    """Headers applied to every v3 response carrying authority-bound data.

    SEC-4: prevents intermediate caches from retaining route snapshots.
    ``private`` is redundant when ``no-store`` is set, but keeping both
    makes the intent explicit for older proxies that honour the weaker
    directive.
    """

    return {"Cache-Control": "no-store, private"}


@auth_required("worker")
async def get_routes(request: web.Request) -> web.StreamResponse:
    """``GET /api/v3/worker/{worker_id}/routes``.

    Returns the unified route snapshot (200) or an empty 304 when the worker
    is already up-to-date as determined by the ``If-None-Match`` etag.

    Response always carries:
      * ``Cache-Control: no-store, private`` (SEC-4)
      * ``ETag`` — current snapshot fingerprint
      * ``X-Route-Version`` — monotonically increasing route_version
    """

    root_ctx: RootContext = request.app["_root.context"]
    worker_id = UUID(request.match_info["worker_id"])

    if_none_match = request.headers.get("If-None-Match")

    service = RouteSnapshotService(root_ctx.db, root_ctx.valkey_live)
    snapshot = await service.get_snapshot(
        worker_id=worker_id,
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


@auth_required("worker")
async def heartbeat_v3(request: web.Request) -> web.StreamResponse:
    """``PATCH /api/v3/worker/{worker_id}/heartbeat``.

    Workers — push or polling — send an empty body as a plain liveness
    ping. The handler refreshes ``last_polled_at`` on the row and the
    ``proxy-worker.last_seen`` Valkey hash that ``check_worker_lost``
    consults, and promotes the worker out of LOST if it had been marked.
    """

    root_ctx: RootContext = request.app["_root.context"]
    worker_id = UUID(request.match_info["worker_id"])

    now = datetime.now(UTC)

    async def _update(sess: SASession) -> str:
        worker = await Worker.get(sess, worker_id)
        worker.last_polled_at = now
        if worker.status != WorkerStatus.ALIVE:
            worker.status = WorkerStatus.ALIVE
        return worker.authority

    async with root_ctx.db.connect() as db_conn:
        authority = await execute_with_txn_retry(_update, root_ctx.db.begin_session, db_conn)

    ttl = get_default_redis_key_ttl()
    await root_ctx.valkey_live.hset_with_expiry(
        "proxy-worker.last_seen",
        {authority: str(now.timestamp())},
        ttl,
    )

    request["do_not_print_access_log"] = True
    return web.json_response(
        WorkerHeartbeatResponse(
            ack_at=now,
            poll_interval_hint_ms=_DEFAULT_POLL_INTERVAL_HINT_MS,
        ).model_dump(mode="json"),
        headers=_no_store_headers(),
    )


@auth_required("worker")
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


@auth_required("worker")
async def register_v3(request: web.Request) -> web.StreamResponse:
    """``POST /api/v3/worker``.

    v3 worker registration. Auth is the same ``X-BackendAI-Token:
    <api_secret>`` header used by every other Worker ↔ Coordinator
    endpoint (v2 included); no separate poll-token issuance.

    Request body is :class:`WorkerRegistrationRequest` — the SAME wire
    contract spoken by the legacy v2 ``PUT /api/worker`` endpoint. The
    two endpoints differ only in the response shape: v2 returns a full
    ``WorkerResponseModel`` (worker row + slots) so older Python workers
    can refresh their cached slot list, while v3 returns the minimal
    :class:`WorkerRegistrationResponseV3` (worker id only).
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
    # backend_kind, in which case CONTINUUM is the inferred default for
    # this endpoint. ``WorkerMode`` (self-hosted vs external-backend) is
    # derived from ``backend_kind`` via :attr:`BackendKind.worker_mode`
    # and is not stored as a separate column.
    backend_kind = params.backend_kind or BackendKind.CONTINUUM

    initial_status = WorkerStatus.ALIVE
    accepted_traffics = list(params.accepted_traffics)
    port_range: tuple[int, int] | None = (
        (params.port_range[0], params.port_range[1]) if params.port_range else None
    )
    authority = params.authority

    async def _upsert(sess: SASession) -> UUID:
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
            worker.updated_at = datetime.now(UTC)
            worker.nodes += 1
            worker.status = initial_status
            return worker.id
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
            sess.add(worker)
            await sess.flush()
            await sess.refresh(worker)
            return worker.id

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

    async def _upsert_and_filters(sess: SASession) -> UUID:
        worker_id = await _upsert(sess)
        await _sync_app_filters(sess, worker_id)
        return worker_id

    async with root_ctx.db.connect() as db_conn:
        worker_id = await execute_with_txn_retry(
            _upsert_and_filters, root_ctx.db.begin_session, db_conn
        )

    log.info("v3 worker registered: authority={} id={}", authority, worker_id)

    return web.json_response(
        WorkerRegistrationResponseV3(
            worker_id=worker_id,
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
    # v3 worker registration — same X-BackendAI-Token auth as v2. The v3
    # protocol equivalent of legacy ``PUT /api/worker``.
    root_resource = cors.add(app.router.add_resource(r""))
    cors.add(root_resource.add_route("POST", register_v3))
    cors.add(add_route("DELETE", "/{worker_id}", deregister_v3))
    cors.add(add_route("GET", "/{worker_id}/routes", get_routes))
    cors.add(add_route("PATCH", "/{worker_id}/heartbeat", heartbeat_v3))
    cors.add(add_route("GET", "/{worker_id}/health", health))
    return app, []
