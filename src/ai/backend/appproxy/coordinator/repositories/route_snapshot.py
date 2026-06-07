"""Repository layer for assembling a worker's route snapshot.

Owns the DB engine and the transaction boundary. Public methods are
logical units that span one or more ORM operations inside a single
read-only transaction so the caller (service layer) only deals with
business logic, never with ``begin_readonly_session`` plumbing.

Worker-scoped: every query filters on ``worker_id`` only. Cross-worker
leak surface is zero by construction — there are no other filter
parameters, so a snapshot built here can never include data for a
different worker even if a caller passes the wrong ID upstream
(P0 SEC-2 alignment).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.orm import selectinload

from ai.backend.appproxy.coordinator.models import Circuit, Endpoint, Worker
from ai.backend.appproxy.coordinator.models.utils import ExtendedAsyncSAEngine
from ai.backend.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

__all__ = [
    "RouteSnapshotData",
    "RouteSnapshotRepository",
]


@dataclass(slots=True)
class RouteSnapshotData:
    """Raw ORM rows assembled for one worker's snapshot.

    The service layer projects these into the wire-format snapshot DTO;
    keeping the repository output as raw rows lets the projection logic
    stay in one place and keeps this layer free of DTO coupling.

    ``worker_row`` is ``None`` when the requested worker does not exist
    so the caller can decide whether that is a 404 or a no-op.
    ``routes_rows`` is reserved for a future routes table; today the
    per-route information lives inside ``Circuit.route_info`` (JSONB).
    """

    worker_row: Worker | None
    circuits_rows: list[Circuit] = field(default_factory=list)
    routes_rows: list[object] = field(default_factory=list)
    endpoints_rows: list[Endpoint] = field(default_factory=list)


class RouteSnapshotRepository:
    _db: ExtendedAsyncSAEngine

    def __init__(self, db: ExtendedAsyncSAEngine) -> None:
        self._db = db

    # ---- Public (transactional) API ----

    async def fetch_routes_for_worker(self, worker_id: UUID) -> RouteSnapshotData:
        """Assemble the full route snapshot for one worker in one read-only tx.

        Only ``worker_id`` is used as a filter on every query so the
        snapshot cannot contain rows belonging to a different worker
        (P0 SEC-2 alignment). The circuits query eagerly loads
        ``worker_row`` and ``endpoint_row`` via ``selectinload`` so the
        service layer can project the snapshot after the transaction
        closes without raising ``DetachedInstanceError``.
        """
        async with self._db.begin_readonly_session() as sess:
            worker_row = await sess.scalar(sa.select(Worker).where(Worker.id == worker_id))
            if worker_row is None:
                return RouteSnapshotData(worker_row=None)

            circuit_rows = (
                await sess.scalars(
                    sa.select(Circuit)
                    .where(Circuit.worker == worker_id)
                    .options(
                        selectinload(Circuit.worker_row),
                        selectinload(Circuit.endpoint_row),
                    )
                )
            ).all()

            # Endpoints are only meaningful for circuits attached to a
            # deployment; collect them from the already-loaded circuit
            # rows to avoid a second worker-scoped query that would
            # widen the filter surface.
            endpoint_rows: list[Endpoint] = [
                circuit.endpoint_row for circuit in circuit_rows if circuit.endpoint_row is not None
            ]

            # ``routes_rows`` is reserved for a dedicated routes table
            # that does not exist yet — today the per-route entries are
            # embedded in ``Circuit.route_info`` (JSONB) and the service
            # layer reads them straight off ``circuits_rows``. We keep
            # the field in the dataclass so the consumer contract stays
            # stable once the table lands.
            routes_rows: list[object] = []

            return RouteSnapshotData(
                worker_row=worker_row,
                circuits_rows=list(circuit_rows),
                routes_rows=routes_rows,
                endpoints_rows=endpoint_rows,
            )
