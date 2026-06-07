"""Opaque secret reference resolution for worker mode (OB-COMP-1 mitigation).

This service is the single, narrowly-scoped place that converts opaque
``secret://worker/{worker_id}/{kind}`` references (the only secret-shaped
strings the v3 route snapshot embeds — see SEC-4 in
``ai.backend.appproxy.common.dto.v3.route_snapshot``) back into the raw
secret material a worker needs immediately before dispatching an upstream
request.

Why a dedicated resolver (open question OB-COMP-1):
    The original prototype shipped upstream credentials (e.g. external LLM
    API keys for OpenAI / Anthropic / Bedrock backends) inline in the
    snapshot payload. That makes every snapshot — and every log line,
    debug dump, on-disk crash-restart cache, or transient breach that
    captures the snapshot — a credential exfiltration surface. The OB-COMP-1
    decision is option (b): keep the snapshot free of secret values and
    introduce a dedicated resolution endpoint backed by this service. The
    benefits are:

    * Snapshots can be cached, replayed, and logged without redaction.
    * Credential rotation does not bump ``route_version`` or re-fan the
      snapshot to every worker — only the next ``resolve()`` returns the
      new value.
    * The resolution endpoint is the *only* surface that needs hardening,
      auditing, and rate limiting.

Reference shape (must match ``RouteSnapshotService._secret_ref``):

    ``secret://worker/{worker_id}/{kind}``

Where ``kind`` is one of:

    * ``jwt_verification``  — JWT verification secret used by the
      Traefik plugin and worker-side request validation.
    * ``permit_hash``       — HMAC secret used to validate permit hashes
      that the manager attaches to admin requests.
    * ``inference/{deployment_id}`` — per-deployment inference JWT used
      when the upstream backend is an external LLM service. Stubbed today
      (the deployments table does not yet store these tokens); the resolver
      returns ``None`` so the caller can surface a 404 instead of leaking
      uninitialised state.

Cross-worker rejection (SEC-2 alignment):
    The resolver requires the caller-supplied ``worker_id`` to match the
    ``worker_id`` embedded in the reference URI. A worker A token may
    therefore not be replayed to resolve a worker B reference even if the
    raw URI string somehow leaked — defending in depth alongside the
    auth_required("worker_v3") scope check on the endpoint.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import unquote
from uuid import UUID

from ai.backend.logging import BraceStyleAdapter

if TYPE_CHECKING:
    from ai.backend.appproxy.coordinator.types import RootContext

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

__all__ = (
    "ParsedSecretRef",
    "SecretResolver",
    "SecretRefParseError",
)


_SCHEME_PREFIX = "secret://worker/"


class SecretRefParseError(ValueError):
    """Raised when a secret reference URI does not conform to the expected shape.

    Kept as a ``ValueError`` subclass so callers that simply want to reject
    a malformed reference can ``except ValueError`` without importing this
    module. The endpoint handler converts this into a 400.
    """


@dataclass(slots=True, frozen=True)
class ParsedSecretRef:
    """Structured form of ``secret://worker/{worker_id}/{kind}``.

    ``worker_id`` is the UUID of the worker the reference is bound to (SEC-2
    cross-worker replay defence). ``kind`` is the top-level secret kind (e.g.
    ``jwt_verification``, ``permit_hash``, ``inference``). ``sub_path`` carries
    any remaining path segments after ``kind`` — for ``inference/{deployment_id}``
    this is the deployment id; for the simple kinds it is ``None``.
    """

    worker_id: UUID
    kind: str
    sub_path: str | None


def parse_secret_ref(ref: str) -> ParsedSecretRef:
    """Parse a ``secret://worker/{worker_id}/{kind}[/{sub_path}]`` URI.

    The reference is URL-decoded once (the endpoint receives the ``ref`` as
    a URL-encoded path parameter) and validated against the expected scheme
    prefix. Any deviation raises :class:`SecretRefParseError` so the caller
    can return a 400 without leaking the malformed value into structured
    logs.
    """
    if not ref:
        raise SecretRefParseError("empty secret reference")
    decoded = unquote(ref)
    if not decoded.startswith(_SCHEME_PREFIX):
        raise SecretRefParseError(f"unexpected scheme; expected prefix '{_SCHEME_PREFIX}'")
    remainder = decoded[len(_SCHEME_PREFIX) :]
    parts = remainder.split("/", 2)
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise SecretRefParseError("reference missing worker_id or kind segment")
    try:
        worker_id = UUID(parts[0])
    except ValueError as exc:
        raise SecretRefParseError("worker_id segment is not a valid UUID") from exc
    kind = parts[1]
    sub_path = parts[2] if len(parts) == 3 and parts[2] else None
    return ParsedSecretRef(worker_id=worker_id, kind=kind, sub_path=sub_path)


class SecretResolver:
    """Resolves opaque secret refs (``secret://worker/{wid}/{kind}``) to raw values.

    Refs are bound to a worker_id; cross-worker resolution is rejected
    (SEC-2 + OB-COMP-1 alignment).

    Known kinds (initial set; extend deliberately as new external backends
    land):

      * ``jwt_verification`` — the route snapshot's JWT verification secret
        (sourced from ``root_ctx.local_config.secrets.jwt_secret``).
      * ``permit_hash`` — Traefik plugin permit-hash secret (sourced from
        ``root_ctx.local_config.permit_hash.secret``).
      * ``inference/{deployment_id}`` — per-deployment inference JWT for
        external LLM backends. Returns ``None`` today because the
        coordinator's ``endpoints`` table does not yet carry these tokens.
        The TODO is intentional and tracked under OB-COMP-1 follow-up.

    The resolver intentionally does not cache values: rotation latency is
    bounded by the next ``resolve()`` call, and the secret material never
    needs to outlive a single request. Callers MUST drop the returned
    string immediately after use (the endpoint handler clears its local
    reference and disables response caching via ``Cache-Control: no-store,
    private``).
    """

    _root_ctx: RootContext

    def __init__(self, root_ctx: RootContext) -> None:
        self._root_ctx = root_ctx

    async def resolve(self, worker_id: UUID, ref: str) -> str | None:
        """Resolve ``ref`` against ``worker_id``; return raw secret or ``None``.

        Returns ``None`` when:
          * the reference targets a known kind that has no current value
            (e.g. ``inference/{deployment_id}`` — stubbed pending the
            deployments-table schema update); or
          * the reference targets an *unknown* kind. ``None`` is a safer
            default than an exception here because it lets the endpoint
            uniformly map "no secret available" to a 404 without leaking
            which kinds are recognised.

        Raises :class:`SecretRefParseError` on malformed URIs and
        :class:`PermissionError` when the ref's worker_id segment does not
        match the caller-supplied ``worker_id`` (SEC-2 cross-worker replay
        defence). The endpoint converts these into 400 and 403 respectively.
        """
        parsed = parse_secret_ref(ref)

        # SEC-2: defence-in-depth against cross-worker reference resolution.
        # Even if a worker_v3 token for worker A somehow obtained a reference
        # string bound to worker B, this check stops the resolution before
        # any secret material is read out of config or the database.
        if parsed.worker_id != worker_id:
            log.warning(
                "Cross-worker secret resolution rejected: path worker_id={} "
                "but ref worker_id={} (kind={})",
                worker_id,
                parsed.worker_id,
                parsed.kind,
            )
            raise PermissionError("secret reference bound to a different worker")

        kind = parsed.kind
        if kind == "jwt_verification":
            secret = self._root_ctx.local_config.secrets.jwt_secret
            return secret or None
        if kind == "permit_hash":
            raw = self._root_ctx.local_config.permit_hash.secret
            if not raw:
                return None
            # ``PermitHashConfig.secret`` is stored as bytes; the consumer
            # (Traefik plugin) expects the same text encoding the rest of
            # the codebase uses when emitting permit material.
            return raw.decode("utf-8")
        if kind == "inference":
            # TODO(OB-COMP-1): wire up real inference JWT lookup once the
            # coordinator's deployments / endpoints table carries the per-
            # deployment external-LLM API token. Today the endpoint table
            # has no such column, so we return ``None`` — the endpoint
            # handler surfaces this as a 404 to the worker, which then
            # falls back to the existing "credential missing" error path
            # instead of dispatching with an empty Authorization header.
            deployment_id_raw = parsed.sub_path
            if not deployment_id_raw:
                raise SecretRefParseError("inference reference requires a deployment_id sub-path")
            try:
                deployment_id = UUID(deployment_id_raw)
            except ValueError as exc:
                raise SecretRefParseError("inference deployment_id is not a valid UUID") from exc
            log.debug(
                "Inference token lookup stubbed for worker={} deployment={} (OB-COMP-1 follow-up)",
                worker_id,
                deployment_id,
            )
            return None

        # Unknown kind. Log at debug to avoid noisy operator alerts on a
        # mistyped reference but do not leak the raw ref in higher-level
        # logs.
        log.debug(
            "Unknown secret kind '{}' for worker={} — returning None",
            kind,
            worker_id,
        )
        return None
