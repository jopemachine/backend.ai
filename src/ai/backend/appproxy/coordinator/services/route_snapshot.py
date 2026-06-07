"""Worker route snapshot assembly service.

Why content-hash ``route_version`` not Postgres sequence (P1 RC-1):
    A monotonic Postgres ``nextval`` sequence served as the original
    ``route_version`` source — every call to ``get_snapshot`` would burn
    a new value even when nothing about the worker's routing tree had
    actually changed. That non-determinism breaks ETag caching for
    workers polling at a high cadence: identical bytes get different
    versions, the worker's ``If-None-Match`` never matches, and the
    coordinator pays the full snapshot serialization cost on every
    request. Mitigation (RC-1): derive ``route_version`` deterministically
    from a blake2b digest of the canonical-JSON form of the snapshot's
    raw data. Equal inputs produce equal versions; the etag is therefore
    stable across calls until the routing tree actually mutates, and
    304s become trustworthy.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.orm import selectinload

from ai.backend.appproxy.common.dto.v3.route_snapshot import (
    EndpointDTO,
    ReplicaDTO,
)
from ai.backend.appproxy.coordinator.models import Circuit, Endpoint, Worker
from ai.backend.appproxy.coordinator.models.utils import ExtendedAsyncSAEngine
from ai.backend.common.clients.valkey_client.valkey_live.client import ValkeyLiveClient
from ai.backend.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

__all__ = [
    "RouteSnapshotResult",
    "RouteSnapshotService",
    "SnapshotFormat",
]


SnapshotFormat = Literal["unified"]


_SNAPSHOT_ETAG_CACHE_TTL_SEC: int = 600
_SNAPSHOT_ETAG_CACHE_KEY_PREFIX: str = "proxy-worker.snapshot_etag:"


@dataclass(slots=True)
class RouteSnapshotResult:
    """Outcome of a snapshot assembly request.

    ``status_code`` is ``304`` when the caller's ``etag_hint`` matches
    the freshly computed etag (cache hit); otherwise it is ``200`` and
    ``body`` carries the formatted snapshot. ``route_version`` is the
    content-hash derived version (see module docstring, RC-1) and is
    safe to publish even on 304 so the worker can update its local
    bookkeeping. ``generated_at`` is the assembly timestamp in UTC.
    """

    status_code: Literal[200, 304]
    body: dict[str, Any] | None
    etag: str
    route_version: int
    generated_at: datetime


class RouteSnapshotService:
    """Assemble per-worker route snapshots from coordinator state.

    The service owns the etag computation and format dispatch but
    delegates DB I/O to a single read-only session opened directly on
    the engine — every query is filtered on ``worker_id`` so the
    snapshot cannot contain rows from a different worker (P0 SEC-2
    alignment, RC-2 single-session guarantee).
    """

    _db: ExtendedAsyncSAEngine
    _valkey: ValkeyLiveClient

    def __init__(self, db: ExtendedAsyncSAEngine, valkey: ValkeyLiveClient) -> None:
        self._db = db
        self._valkey = valkey

    # ---- Public API ----

    async def get_snapshot(
        self,
        worker_id: UUID,
        format: SnapshotFormat = "unified",
        since: int | None = None,
        etag_hint: str | None = None,
    ) -> RouteSnapshotResult:
        """Assemble a snapshot for ``worker_id`` in the requested ``format``.

        Steps:
            1. Open ONE read-only session and read worker + circuits
               (with their endpoints) in one round-trip.
            2. Build a deterministic canonical representation of the
               routing data.
            3. Derive ``route_version`` and ``etag`` from that
               canonical form (RC-1).
            4. Short-circuit with ``304`` when ``etag_hint`` matches.
            5. Otherwise render the unified body.
            6. Cache the etag in valkey for downstream lookups.

        ``since`` is accepted for future delta-style responses; today
        the snapshot is always full-body and the value is ignored. It
        stays in the signature so callers do not need to be revisited
        when delta support lands.
        """
        del since  # reserved for future delta-snapshot support
        del format  # only ``unified`` is supported today; reserved for future variants.

        # (1) Single read-only session covering every row we need.
        async with self._db.begin_readonly_session() as sess:
            worker_row = await sess.scalar(sa.select(Worker).where(Worker.id == worker_id))
            if worker_row is None:
                # Worker missing: emit an empty deterministic snapshot
                # so polling workers see a stable shape rather than a
                # crash. The etag will be stable for any subsequent
                # poll until the worker registers.
                circuit_rows: list[Circuit] = []
                endpoint_rows: list[Endpoint] = []
            else:
                circuit_rows = list(
                    (
                        await sess.scalars(
                            sa.select(Circuit)
                            .where(Circuit.worker == worker_id)
                            .options(
                                selectinload(Circuit.worker_row),
                                selectinload(Circuit.endpoint_row),
                            )
                        )
                    ).all()
                )
                endpoint_rows = [
                    circuit.endpoint_row
                    for circuit in circuit_rows
                    if circuit.endpoint_row is not None
                ]

        generated_at = datetime.now(UTC)

        # (2) Canonical representation — sorted-by-id, primitive-only.
        routes_canonical = self._canonical_routes_payload(circuit_rows, endpoint_rows)
        routes_canonical_bytes = self._canonical_json(routes_canonical)

        # (3) Content-hash route_version (RC-1: avoid nextval non-determinism).
        route_version = int.from_bytes(
            hashlib.blake2b(routes_canonical_bytes, digest_size=8).digest(),
            byteorder="big",
            signed=False,
        )
        etag = f"v{route_version}"

        # (4) Cache the etag in valkey so adjacent endpoints (e.g. a
        # cheap HEAD-style probe) can short-circuit without rebuilding
        # the snapshot. Failure to cache is non-fatal: the snapshot is
        # already valid and the next call will recompute.
        cache_key = f"{_SNAPSHOT_ETAG_CACHE_KEY_PREFIX}{worker_id}"
        try:
            await self._valkey.store_live_data(cache_key, etag, ex=_SNAPSHOT_ETAG_CACHE_TTL_SEC)
        except Exception as exc:  # pragma: no cover - best-effort cache
            log.debug("snapshot etag cache failed for {}: {}", worker_id, exc)

        # (5) Short-circuit on etag match.
        if etag_hint is not None and etag_hint == etag:
            return RouteSnapshotResult(
                status_code=304,
                body=None,
                etag=etag,
                route_version=route_version,
                generated_at=generated_at,
            )

        # (6) Render the unified body.
        body = self._format_unified(
            circuit_rows=circuit_rows,
            endpoint_rows=endpoint_rows,
            route_version=route_version,
            generated_at=generated_at,
        )

        return RouteSnapshotResult(
            status_code=200,
            body=body,
            etag=etag,
            route_version=route_version,
            generated_at=generated_at,
        )

    # ---- Canonicalization helpers ----

    @staticmethod
    def _canonical_json(payload: Any) -> bytes:
        """Serialize ``payload`` to a deterministic byte string.

        ``sort_keys=True`` + ``separators`` + no NaN handling keeps the
        output byte-for-byte identical across runs and processes so the
        downstream digest is reproducible (RC-1 prerequisite).
        """
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=str,
        ).encode("utf-8")

    @staticmethod
    def _canonical_routes_payload(
        circuit_rows: list[Circuit],
        endpoint_rows: list[Endpoint],
    ) -> dict[str, Any]:
        """Build the primitive-only routes view used for the digest.

        Sorting by ``id`` makes the result independent of DB row order
        so the digest is stable across replicas and after VACUUMs.
        """
        circuits_view: list[dict[str, Any]] = []
        for circuit in sorted(circuit_rows, key=lambda c: str(c.id)):
            route_info_dump: list[dict[str, Any]] = []
            for route in sorted(circuit.route_info or [], key=lambda r: str(r.route_id)):
                route_info_dump.append(route.model_dump(mode="json"))
            circuits_view.append({
                "id": str(circuit.id),
                "app": circuit.app,
                "protocol": str(circuit.protocol),
                "app_mode": str(circuit.app_mode),
                "frontend_mode": str(circuit.frontend_mode),
                "port": circuit.port,
                "subdomain": circuit.subdomain,
                "endpoint_id": (str(circuit.endpoint_id) if circuit.endpoint_id else None),
                "runtime_variant": circuit.runtime_variant,
                "route_info": route_info_dump,
            })

        endpoints_view: list[dict[str, Any]] = []
        for endpoint in sorted(endpoint_rows, key=lambda e: str(e.id)):
            health_check_config = (
                endpoint.health_check_config.model_dump(mode="json")
                if endpoint.health_check_config is not None
                else None
            )
            endpoints_view.append({
                "id": str(endpoint.id),
                "health_check_enabled": endpoint.health_check_enabled,
                "health_check_config": health_check_config,
            })

        return {
            "circuits": circuits_view,
            "endpoints": endpoints_view,
        }

    # ---- Formatter ----

    def _format_unified(
        self,
        *,
        circuit_rows: list[Circuit],
        endpoint_rows: list[Endpoint],
        route_version: int,
        generated_at: datetime,
    ) -> dict[str, Any]:
        """Unified cross-backend snapshot shape."""
        endpoint_dtos: list[EndpointDTO] = []
        endpoint_by_id: dict[UUID, Endpoint] = {endpoint.id: endpoint for endpoint in endpoint_rows}

        for circuit in sorted(circuit_rows, key=lambda c: str(c.id)):
            endpoint_id = circuit.endpoint_id
            if endpoint_id is None:
                continue
            endpoint = endpoint_by_id.get(endpoint_id)
            if endpoint is None:
                continue
            endpoint_dtos.append(self._build_endpoint_dto(circuit=circuit, endpoint=endpoint))

        return {
            "route_version": route_version,
            "issued_at": generated_at.isoformat(),
            "endpoints": [dto.model_dump(mode="json") for dto in endpoint_dtos],
        }

    def _build_endpoint_dto(
        self,
        *,
        circuit: Circuit,
        endpoint: Endpoint,
    ) -> EndpointDTO:
        """Assemble one v3 ``EndpointDTO`` from a circuit + endpoint pair."""
        primary_model_name = str(endpoint.id)

        replicas: list[ReplicaDTO] = []
        for route in sorted(circuit.route_info or [], key=lambda r: str(r.route_id)):
            if route.route_id is None:
                continue
            replicas.append(
                ReplicaDTO(
                    replica_id=route.route_id,
                    kernel_host=route.current_kernel_host,
                    kernel_port=route.kernel_port,
                    model_name=primary_model_name,
                )
            )

        return EndpointDTO(
            endpoint_id=endpoint.id,
            app=circuit.app or primary_model_name,
            replicas=replicas,
        )
