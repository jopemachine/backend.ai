import logging
import uuid
from collections.abc import Sequence
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    pass

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pgsql
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload
from yarl import URL

from ai.backend.appproxy.common.errors import (
    ObjectNotFound,
    PortNotAvailable,
    WorkerNotAvailable,
)
from ai.backend.appproxy.common.types import (
    AppMode,
    BackendKind,
    EndpointConfig,
    FrontendMode,
    ProxyProtocol,
    RouteInfo,
    SessionConfig,
    Slot,
)
from ai.backend.appproxy.coordinator.errors import MissingFrontendConfigError
from ai.backend.logging import BraceStyleAdapter

from .base import GUID, Base, BaseMixin, EnumType, StrEnumType
from .circuit import Circuit

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

__all__ = [
    "Worker",
    "WorkerAppFilter",
    "WorkerStatus",
    "_is_polling_ready",
    "add_circuit",
    "is_v3_polling_active",
    "pick_worker",
]


def _is_polling_worker(worker: "Worker") -> bool:
    """Polling-worker check. CONTINUUM is the only polling backend today;
    NATIVE / TRAEFIK keep the v2 push channel. When NATIVE/TRAEFIK go
    polling in a future iteration the check expands here in one place.
    """
    return worker.backend_kind == BackendKind.CONTINUUM


def is_v3_polling_active(worker: "Worker") -> bool:
    """Return True iff the worker has fully cut over to v3 snapshot polling.

    OB-OPS-1 rollout-ordering mitigation:
        A polling worker registers via the v3 endpoint, but the coordinator
        cannot stop emitting legacy ``AppProxyCircuit*Event`` broadcasts to
        that worker until the worker has actually pulled and applied at
        least one snapshot. Until then, the worker still depends on the
        event stream to learn about circuits, and silently dropping the
        events would leave it with an empty / stale route table.

        We therefore require BOTH:
          1. The worker is a polling worker (``backend_kind == CONTINUUM``).
          2. ``committed_route_version > 0`` — the worker has reported back
             at least one successfully applied snapshot, so it is now the
             authoritative source for its own routing state and the legacy
             event channel can be safely silenced.
    """
    if not _is_polling_worker(worker):
        return False
    return worker.committed_route_version > 0


def _is_polling_ready(worker: "Worker") -> bool:
    """OB-COMP-4 mitigation: only polling workers must show
    ``committed_route_version > 0`` before they become candidates for
    ``pick_worker``. Push workers (NATIVE / TRAEFIK) never poll, so their
    ``committed_route_version`` stays at 0 — applying the freshness gate
    uniformly would permanently exclude them from circuit assignment.
    """
    if not _is_polling_worker(worker):
        return True
    return worker.committed_route_version > 0


class WorkerStatus(StrEnum):
    # OPS-2 mitigation: STARTING marks a worker that has registered but has
    # not yet confirmed it can serve traffic. v3-polling workers stay in
    # STARTING until the Continuum subprocess applies its first snapshot
    # (signalled by ``applied_route_version > 0`` in the heartbeat). The
    # ``pick_worker`` candidate set excludes STARTING workers so that the
    # cold-start gap between registration and first applied snapshot cannot
    # silently route traffic to a worker whose data-plane is empty.
    STARTING = "STARTING"
    ALIVE = "ALIVE"
    LOST = "LOST"
    TERMINATED = "TERMINATED"


class Worker(Base, BaseMixin):  # type: ignore[misc]
    __tablename__ = "workers"

    id: Mapped[UUID] = mapped_column(
        GUID, primary_key=True, server_default=sa.text("uuid_generate_v4()")
    )

    authority: Mapped[str] = mapped_column(
        sa.String(length=255), nullable=False, unique=True
    )  # Human-readable identity of each AppProxy worker, must be identical across all AppProxy workers under single VIP when HA is set up.
    frontend_mode: Mapped[FrontendMode] = mapped_column(
        EnumType(FrontendMode), nullable=False
    )  # will be fixed as `port` when `protocol` is set to `tcp`
    protocol: Mapped[ProxyProtocol] = mapped_column(
        EnumType(ProxyProtocol), nullable=False
    )  # type of traffic to procy

    hostname: Mapped[str] = mapped_column(
        sa.String(length=1024), nullable=False
    )  # Hostname which users utilize to access this AppProxy
    tls_listen: Mapped[bool] = mapped_column(
        sa.Boolean(), default=False, nullable=False
    )  # Indicates if TLS is required to access the AppProxy
    tls_advertised: Mapped[bool] = mapped_column(
        sa.Boolean(), default=False, nullable=False
    )  # Indicates if TLS is required to access the AppProxy

    api_port: Mapped[int] = mapped_column(sa.Integer(), nullable=False)  # REST API port

    available_slots: Mapped[int] = mapped_column(
        sa.Integer(), default=0, nullable=False
    )  # set to -1 when `frontend_mode` is set to `wildcard`
    occupied_slots: Mapped[int] = mapped_column(sa.Integer(), default=0, nullable=False)

    # Only set if `frontend_mode` is `port`
    port_range: Mapped[tuple[int, int] | None] = mapped_column(
        pgsql.ARRAY(sa.Integer), nullable=True
    )

    # Only set if `frontend_mode` is `wildcard`
    # .example.com
    wildcard_domain: Mapped[str | None] = mapped_column(sa.String(length=1024), nullable=True)
    # Only set if `frontend_mode` is `wildcard`
    wildcard_traffic_port: Mapped[int | None] = mapped_column(sa.Integer(), nullable=True)

    nodes: Mapped[int] = mapped_column(sa.Integer(), default=1, nullable=False)

    accepted_traffics: Mapped[list[AppMode]] = mapped_column(
        pgsql.ARRAY(EnumType(AppMode)), nullable=False
    )
    filtered_apps_only: Mapped[bool] = mapped_column(sa.Boolean(), default=False, nullable=False)

    traefik_last_used_marker_path: Mapped[str | None] = mapped_column(
        sa.String(length=1024), nullable=True
    )

    created_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )

    status: Mapped[WorkerStatus] = mapped_column(
        StrEnumType(WorkerStatus),
        default=WorkerStatus.ALIVE,
        nullable=False,
    )

    # The concrete data-plane backend kind. NATIVE is self-hosted (the
    # worker's own listener handles traffic); TRAEFIK and CONTINUUM both
    # drive an external data-plane component. The legacy ``mode`` column
    # (``self-hosted`` / ``external-backend``) was retired — that
    # distinction is derived from this value via
    # :attr:`BackendKind.worker_mode`.
    backend_kind: Mapped[BackendKind] = mapped_column(
        StrEnumType(BackendKind),
        default=BackendKind.NATIVE,
        server_default=BackendKind.NATIVE.value,
        nullable=False,
    )

    committed_route_version: Mapped[int] = mapped_column(
        sa.BigInteger,
        default=0,
        server_default=sa.text("0"),
        nullable=False,
    )  # Last route_version the worker reported as applied — gates pick_worker

    last_polled_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
    )

    filters = relationship(
        "WorkerAppFilter",
        back_populates="worker_row",
    )
    circuits = relationship(
        "Circuit",
        back_populates="worker_row",
    )

    @classmethod
    async def get(
        cls,
        session: AsyncSession,
        worker_id: UUID,
        load_filters: bool = False,
        load_circuits: bool = False,
    ) -> "Worker":
        query = sa.select(Worker).filter(Worker.id == worker_id)
        if load_filters:
            query = query.options(selectinload(Worker.filters))
        if load_circuits:
            query = query.options(selectinload(Worker.circuits))
        worker = await session.scalar(query)
        if not worker:
            raise ObjectNotFound(object_name="worker")
        return worker

    @classmethod
    async def find_by_authority(
        cls,
        session: AsyncSession,
        authority: str,
        load_filters: bool = False,
        load_circuits: bool = False,
    ) -> "Worker":
        query = sa.select(Worker).filter(Worker.authority == authority)
        if load_filters:
            query = query.options(selectinload(Worker.filters))
        if load_circuits:
            query = query.options(selectinload(Worker.circuits))
        worker = await session.scalar(query)
        if not worker:
            raise ObjectNotFound(object_name="worker")
        return worker

    @classmethod
    async def list_workers(
        cls,
        session: AsyncSession,
        load_filters: bool = False,
        load_circuits: bool = False,
    ) -> list["Worker"]:
        query = sa.select(Worker)
        if load_filters:
            query = query.options(selectinload(Worker.filters))
        if load_circuits:
            query = query.options(selectinload(Worker.circuits))
        worker = await session.scalars(query)
        return list(worker)

    @classmethod
    def create(
        cls,
        id: UUID,
        authority: str,
        frontend_mode: FrontendMode,
        protocol: ProxyProtocol,
        hostname: str,
        tls_listen: bool,
        tls_advertised: bool,
        api_port: int,
        accepted_traffics: list[AppMode],
        *,
        port_range: tuple[int, int] | None = None,
        wildcard_domain: str | None = None,
        wildcard_traffic_port: int | None = None,
        traefik_last_used_marker_path: str | None = None,
        filtered_apps_only: bool = False,
        status: WorkerStatus = WorkerStatus.LOST,
        backend_kind: BackendKind | None = None,
    ) -> "Worker":
        w = cls()
        w.id = id
        w.authority = authority
        w.frontend_mode = frontend_mode
        w.protocol = protocol
        w.hostname = hostname
        w.tls_listen = tls_listen
        w.tls_advertised = tls_advertised
        w.api_port = api_port
        w.accepted_traffics = accepted_traffics
        w.port_range = port_range
        w.wildcard_domain = wildcard_domain
        w.wildcard_traffic_port = wildcard_traffic_port
        w.filtered_apps_only = filtered_apps_only
        w.traefik_last_used_marker_path = traefik_last_used_marker_path
        w.status = status

        # backend_kind resolution order:
        # (1) explicit backend_kind from the v3-aware worker payload — wins
        #     when present (e.g. CONTINUUM).
        # (2) traefik_last_used_marker_path heuristic — legacy fallback for
        #     pre-v3 Python workers that don't ship backend_kind.
        # (3) default NATIVE.
        if backend_kind is not None:
            w.backend_kind = backend_kind
        elif traefik_last_used_marker_path is not None:
            w.backend_kind = BackendKind.TRAEFIK
        else:
            w.backend_kind = BackendKind.NATIVE

        w.occupied_slots = 0
        match frontend_mode:
            case FrontendMode.WILDCARD_DOMAIN:
                if not wildcard_domain:
                    raise MissingFrontendConfigError(
                        "Wildcard domain is required for WILDCARD_DOMAIN frontend mode"
                    )
                w.available_slots = -1
            case FrontendMode.PORT:
                if not port_range:
                    raise MissingFrontendConfigError(
                        "Port range is required for PORT frontend mode"
                    )
                w.available_slots = port_range[1] - port_range[0] + 1

        return w

    @property
    def use_tls(self) -> bool:
        return self.tls_listen or self.tls_advertised

    @property
    def api_endpoint(self) -> URL:
        should_include_port = (not self.use_tls and self.api_port != 80) or (
            self.use_tls and self.api_port != 443
        )
        base_url = f"{'https' if self.use_tls else 'http'}://{self.hostname}"
        if should_include_port:
            base_url += f":{self.api_port}"
        return URL(base_url)

    async def list_slots(self, session: AsyncSession) -> list[Slot]:
        """
        For workers working with PORT-based frontend_mode, this will list all available and occupied slots
        For workers with SUBDOMAIN, this will list occupied slots only - we can't list all available slots since it is infinite
        """
        circuit_list_query = sa.select(Circuit).where(Circuit.worker == self.id)
        circuits: Sequence[Circuit] = (await session.execute(circuit_list_query)).scalars().all()
        match self.frontend_mode:
            case FrontendMode.PORT:
                if not self.port_range:
                    raise MissingFrontendConfigError(
                        "Port range is required for PORT frontend mode"
                    )
                occupied_ports: dict[int, UUID] = {c.port: c.id for c in circuits if c.port}
                slots = [
                    Slot(
                        FrontendMode.PORT,
                        port in occupied_ports,
                        port=port,
                        subdomain=None,
                        circuit_id=occupied_ports.get(port),
                    )
                    for port in range(self.port_range[0], self.port_range[1] + 1)
                ]
            case FrontendMode.WILDCARD_DOMAIN:
                slots = [
                    Slot(
                        FrontendMode.WILDCARD_DOMAIN,
                        True,
                        port=None,
                        subdomain=c.subdomain,
                        circuit_id=c.id,
                    )
                    for c in circuits
                ]
            case _:
                raise ValueError(f"Invalid frontend mode: {self.frontend_mode}")
        return slots


class WorkerAppFilter(Base, BaseMixin):  # type: ignore[misc]
    __tablename__ = "worker_app_filters"

    id: Mapped[UUID] = mapped_column(
        GUID, primary_key=True, server_default=sa.text("uuid_generate_v4()")
    )

    property_name: Mapped[str] = mapped_column(sa.VARCHAR(96), nullable=False)
    property_value: Mapped[str] = mapped_column(sa.VARCHAR(1024), nullable=False)
    worker: Mapped[UUID] = mapped_column(GUID, sa.ForeignKey("workers.id"), nullable=False)

    worker_row = relationship("Worker", back_populates="filters")

    __table_args__ = (
        sa.UniqueConstraint(
            "worker",
            "property_name",
            "property_value",
            name="uq_worker_app_filter_worker_property_name_property_value",
        ),
    )

    @classmethod
    def create(
        cls, id: UUID, property_name: str, property_value: str, worker: UUID
    ) -> "WorkerAppFilter":
        filter = WorkerAppFilter()
        filter.id = id
        filter.property_name = property_name
        filter.property_value = property_value
        filter.worker = worker
        return filter

    @classmethod
    async def find_by_rule(
        cls, session: AsyncSession, worker: UUID, name: str, value: str
    ) -> "WorkerAppFilter":
        query = sa.select(WorkerAppFilter).where(
            (WorkerAppFilter.worker == worker)
            & (WorkerAppFilter.property_name == name)
            & (WorkerAppFilter.property_value == value)
        )
        rule = await session.scalar(query)
        if not rule:
            raise ObjectNotFound(object_name="worker_app_filter")
        return rule


async def pick_worker(
    session: AsyncSession,
    session_info: SessionConfig,
    endpoint_info: EndpointConfig | None,
    protocol: ProxyProtocol,
    app_mode: AppMode,
) -> Worker:
    app_filter_queries = [
        (WorkerAppFilter.property_name == f"session.{key}")
        & (WorkerAppFilter.property_value == str(value))
        for key, value in session_info.model_dump().items()
    ]
    if endpoint_info:
        app_filter_queries += [
            (WorkerAppFilter.property_name == f"endpoint.{key}")
            & (WorkerAppFilter.property_value == str(value))
            for key, value in endpoint_info.model_dump().items()
        ]
    where_clause = app_filter_queries[0]
    for q in app_filter_queries[1:]:
        where_clause |= q
    query = sa.select(WorkerAppFilter).where(where_clause)
    filter_result = await session.execute(query)
    worker_ids = [app_filter.worker for app_filter in filter_result.scalars().all()]

    log.debug("protocol: {} ({})", protocol, type(protocol))
    # OPS-2: Only ALIVE workers are eligible for circuit assignment.
    # STARTING workers (registered but not yet serving traffic — v3 polling
    # workers awaiting their first applied snapshot) are intentionally
    # excluded here so the Continuum cold-start gap cannot leak traffic to
    # a worker whose data-plane is still empty. LOST/TERMINATED were
    # already excluded by the strict ALIVE match.
    worker_query = sa.select(Worker).where(
        (Worker.protocol == protocol)
        & (Worker.accepted_traffics.contains([app_mode]))
        & (Worker.status == WorkerStatus.ALIVE)
    )
    if worker_ids:
        worker_query = worker_query.where(
            Worker.filtered_apps_only & (Worker.accepted_traffics.contains(app_mode))
        )
    result = await session.execute(worker_query)
    # Sort key (ascending — first element = highest priority):
    #   1. backend_kind priority (CONTINUUM < TRAEFIK < NATIVE) so that
    #      external-backend workers are preferred when available.
    #   2. WILDCARD_DOMAIN frontends ahead of PORT frontends (legacy rule).
    #   3. Within the same group, most-free slot wins (least negative remaining).
    #
    # COMP-4 mitigation: the ``_is_polling_ready`` predicate filters out v3
    # polling workers that have not yet applied a snapshot
    # (committed_route_version == 0) so their empty data-plane cannot
    # receive traffic. Legacy NATIVE / SELF_HOSTED workers never poll, so
    # the predicate intentionally exempts them — gating uniformly on
    # ``committed_route_version > 0`` would permanently exclude every
    # legacy worker (silent regression).
    sorted_workers: list[Worker] = [
        f
        for f in sorted(
            result.scalars().all(),
            key=lambda f: (
                0 if f.frontend_mode == FrontendMode.WILDCARD_DOMAIN else 1,
                (f.occupied_slots - f.available_slots),
            ),
        )
        if f.frontend_mode == FrontendMode.WILDCARD_DOMAIN
        or (f.available_slots - f.occupied_slots) > 0
        if _is_polling_ready(f)  # COMP-4: v3 workers must have applied snapshot; NATIVE exempt.
    ]
    if not sorted_workers:
        raise WorkerNotAvailable
    worker = sorted_workers[0]
    return await Worker.get(
        session,
        worker.id,
        load_circuits=True,
    )


async def add_circuit(
    session: AsyncSession,
    session_info: SessionConfig,
    endpoint_info: EndpointConfig | None,
    app: str,
    protocol: ProxyProtocol,
    mode: AppMode,
    routes: list[RouteInfo],
    *,
    envs: dict[str, Any] | None = None,
    args: str | None = None,
    open_to_public: bool = False,
    allowed_client_ips: str | None = None,
    preferred_port: int | None = None,
    preferred_subdomain: str | None = None,
    worker_id: UUID | None = None,
) -> tuple[Circuit, Worker]:
    if envs is None:
        envs = {}
    if worker_id:
        worker = await Worker.get(session, worker_id, load_circuits=True)
        if worker.available_slots - worker.occupied_slots <= 0 and worker.available_slots >= 0:
            raise WorkerNotAvailable
    else:
        worker = await pick_worker(session, session_info, endpoint_info, protocol, mode)

    circuit_params: dict[str, Any] = {}

    if worker.frontend_mode == FrontendMode.WILDCARD_DOMAIN:
        acquired_subdomains = [c.subdomain for c in worker.circuits]
        if _requested_subdomain := preferred_subdomain:
            subdomain = _requested_subdomain
        else:
            sub_id = str(uuid.uuid4()).split("-")[0]
            subdomain = f"app-{sub_id}"

        while subdomain in acquired_subdomains:
            sub_id = str(uuid.uuid4()).split("-")[0]
            subdomain = f"{_requested_subdomain}-{sub_id}"
        circuit_params["subdomain"] = subdomain
    else:
        acquired_ports = {c.port for c in worker.circuits}
        port_range = worker.port_range
        if not port_range:
            raise MissingFrontendConfigError("Port range is required for PORT frontend mode")
        port_pool = set(range(port_range[0], port_range[1] + 1)) - acquired_ports
        if _requested_port := preferred_port:
            if _requested_port not in port_pool:
                raise PortNotAvailable
            port = _requested_port
        else:
            if len(port_pool) == 0:
                raise PortNotAvailable
            port = port_pool.pop()
        circuit_params["port"] = port

    circuit = Circuit.create(
        uuid.uuid4(),
        app,
        protocol,
        worker.id,
        mode,
        worker.frontend_mode,
        routes,
        envs=envs,
        args=args,
        open_to_public=open_to_public,
        allowed_client_ips=allowed_client_ips,
        user_uuid=session_info.user_uuid,
        endpoint_id=endpoint_info.id if endpoint_info else None,
        runtime_variant=endpoint_info.runtime_variant if endpoint_info else None,
        **circuit_params,
    )
    circuit.worker_row = worker

    worker.occupied_slots += 1
    session.add(circuit)
    return (circuit, worker)
