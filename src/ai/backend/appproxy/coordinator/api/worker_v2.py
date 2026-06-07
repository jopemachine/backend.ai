from __future__ import annotations

import dataclasses
import logging
import textwrap
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Any
from uuid import UUID

import aiohttp_cors
import attrs
from aiohttp import web
from dateutil.tz import tzutc
from pydantic import Field

from ai.backend.appproxy.common.events import DoCheckWorkerLostEvent, WorkerLostEvent
from ai.backend.appproxy.common.types import (
    AppMode,
    CORSOptions,
    FrontendMode,
    ProxyProtocol,
    PydanticResponse,
    SerializableCircuit,
    WebMiddleware,
)
from ai.backend.appproxy.common.utils import (
    pydantic_api_response_handler,
)
from ai.backend.appproxy.coordinator.models import Token, Worker, WorkerStatus
from ai.backend.appproxy.coordinator.types import RootContext
from ai.backend.common.events.dispatcher import EventHandler
from ai.backend.common.types import AgentId, BackendAISchema
from ai.backend.logging import BraceStyleAdapter

from .types import CircuitListResponseModel, SlotModel
from .utils import auth_required

if TYPE_CHECKING:
    pass

log = BraceStyleAdapter(logging.getLogger(__spec__.name))


class WorkerModel(BackendAISchema):
    authority: Annotated[
        str,
        Field(
            description="authority string of worker. Unique across every workers joined on a single coordinator."
        ),
    ]
    frontend_mode: FrontendMode
    protocol: ProxyProtocol

    hostname: str
    tls_listen: bool
    tls_advertised: bool

    api_port: int

    port_range: Annotated[tuple[int, int] | None, Field(default=None)]
    wildcard_domain: Annotated[str | None, Field(default=None)]
    wildcard_traffic_port: Annotated[int | None, Field(default=None)]
    filtered_apps_only: bool
    traefik_last_used_marker_path: Annotated[str | None, Field(default=None)]

    accepted_traffics: list[AppMode]

    # Optional explicit mode / backend_kind sent by v3-aware workers. Older
    # workers omit these; the model defaults to ``None`` and the receiving
    # code falls back to the legacy traefik_marker heuristic.
    mode: Annotated[str | None, Field(default=None)]
    backend_kind: Annotated[str | None, Field(default=None)]
    # OPS-2: v3-polling-aware workers declare their capabilities at
    # registration time. The coordinator uses ``supports_v3_polling`` to
    # decide whether to start the worker in STARTING (must wait for first
    # applied snapshot) or ALIVE (legacy event-driven path is authoritative
    # at boot, so no warm-up window is required).
    capabilities: Annotated[dict[str, Any] | None, Field(default=None)]


class AppFilter(BackendAISchema):
    key: str
    value: str


class WorkerResponseModel(WorkerModel):
    id: Annotated[UUID, Field(description="ID of worker.")]

    created_at: datetime
    updated_at: datetime

    available_slots: Annotated[
        int,
        Field(
            description=textwrap.dedent(
                """
                Number of slots worker is capable to hold. Workers serving `subdomain` frontend have -1 as `available_circuits`.
                For `port` frontend this value is number of ports exposed by the worker.
                """
            ),
        ),
    ]

    occupied_slots: Annotated[int, Field(description="Number of slots occupied by circuit.")]

    nodes: Annotated[
        int,
        Field(
            description="Number of actual nodes claiming as same worker. Can be considered as HA set up if this value is greater than 1.",
        ),
    ]

    slots: list[SlotModel]


class WorkerListResponseModel(BackendAISchema):
    workers: list[WorkerResponseModel]


class TokenResponseModel(BackendAISchema):
    login_session_token: str | None
    kernel_host: str
    kernel_port: int
    session_id: UUID
    user_uuid: UUID
    group_id: UUID
    access_key: str
    domain_name: str


@auth_required("worker")
@pydantic_api_response_handler
async def get_worker(request: web.Request) -> PydanticResponse[WorkerResponseModel]:
    """
    Returns information about worker mentioned.
    """
    root_ctx: RootContext = request.app["_root.context"]

    async with root_ctx.db.begin_readonly_session() as sess:
        worker = await Worker.get(sess, UUID(request.match_info["worker_id"]))
        return PydanticResponse(
            WorkerResponseModel(
                slots=[SlotModel(**dataclasses.asdict(s)) for s in await worker.list_slots(sess)],
                **worker.dump_model(),
            )
        )


@auth_required("worker")
@pydantic_api_response_handler
async def list_workers(request: web.Request) -> PydanticResponse[WorkerListResponseModel]:
    """
    Lists all workers recognized by coordinator.
    """
    root_ctx: RootContext = request.app["_root.context"]

    async with root_ctx.db.begin_readonly_session() as sess:
        workers = await Worker.list_workers(sess)
        return PydanticResponse(
            WorkerListResponseModel(
                workers=[
                    WorkerResponseModel(
                        slots=[
                            SlotModel(**dataclasses.asdict(s)) for s in await w.list_slots(sess)
                        ],
                        **w.dump_model(),
                    )
                    for w in workers
                ]
            )
        )


@auth_required("worker")
@pydantic_api_response_handler
async def list_worker_circuits(request: web.Request) -> PydanticResponse[CircuitListResponseModel]:
    """
    Lists every circuits worker is currently serving.
    """
    root_ctx: RootContext = request.app["_root.context"]

    async with root_ctx.db.begin_readonly_session() as sess:
        worker = await Worker.get(sess, UUID(request.match_info["worker_id"]), load_circuits=True)
        return PydanticResponse(
            CircuitListResponseModel(
                circuits=[SerializableCircuit(**c.dump_model()) for c in worker.circuits]
            )
        )


@auth_required("worker")
@pydantic_api_response_handler
async def get_token(request: web.Request) -> PydanticResponse[TokenResponseModel]:
    """
    Lists tokens issued within /v2/conf request handler.
    """
    root_ctx: RootContext = request.app["_root.context"]
    token_id = UUID(request.match_info["token_id"])

    async with root_ctx.db.begin_readonly_session() as sess:
        token = await Token.get(sess, token_id)
        return PydanticResponse(TokenResponseModel(**token.dump_model()))


async def check_worker_lost(
    app: web.Application, _src: AgentId, _event: DoCheckWorkerLostEvent
) -> None:
    root_ctx: RootContext = app["_root.context"]

    try:
        now = datetime.now(tzutc())
        timeout = timedelta(
            seconds=root_ctx.local_config.proxy_coordinator.worker_heartbeat_timeout
        )

        msg_data = await root_ctx.valkey_live.hgetall_str("proxy-worker.last_seen")

        async with root_ctx.db.begin_readonly_session() as sess:
            workers = await Worker.list_workers(sess)
            worker_map = {w.authority: w for w in workers}

        seen_authorities: set[str] = set()
        for worker_id_str, prev_str in msg_data.items():
            seen_authorities.add(worker_id_str)
            prev = datetime.fromtimestamp(float(prev_str), tzutc())
            if (
                (now - prev) > timeout
                and worker_id_str in worker_map
                and worker_map[worker_id_str].status == WorkerStatus.ALIVE
            ):
                await root_ctx.event_producer.anycast_event(
                    WorkerLostEvent(worker_id_str, "heartbeat timeout")
                )
        # ALIVE workers whose last_seen entry expired from Valkey count as
        # lost too — without this, a stale ALIVE row can hold pick_worker
        # slots forever.
        for authority, worker in worker_map.items():
            if worker.status == WorkerStatus.ALIVE and authority not in seen_authorities:
                await root_ctx.event_producer.anycast_event(
                    WorkerLostEvent(authority, "last_seen missing in valkey")
                )
    except Exception:
        log.exception("check_worker_lost(): exception:")


@attrs.define(slots=True, auto_attribs=True, init=False)
class PrivateContext:
    worker_lost_check_evh: EventHandler[web.Application, DoCheckWorkerLostEvent]


async def init(app: web.Application) -> None:
    root_ctx: RootContext = app["_root.context"]
    app_ctx: PrivateContext = app["worker.context"]

    # Scan ALIVE workers (timer is managed by LeaderCron in leader_election_ctx)
    app_ctx.worker_lost_check_evh = root_ctx.event_dispatcher.consume(
        DoCheckWorkerLostEvent,
        app,
        check_worker_lost,
    )


async def shutdown(app: web.Application) -> None:
    root_ctx: RootContext = app["_root.context"]
    app_ctx: PrivateContext = app["worker.context"]
    root_ctx.event_dispatcher.unconsume(app_ctx.worker_lost_check_evh)


def create_app(
    default_cors_options: CORSOptions,
) -> tuple[web.Application, Iterable[WebMiddleware]]:
    app = web.Application()
    app["prefix"] = "api/worker"
    app["worker.context"] = PrivateContext()
    app.on_startup.append(init)
    app.on_shutdown.append(shutdown)
    cors = aiohttp_cors.setup(app, defaults=default_cors_options)
    add_route = app.router.add_route
    root_resource = cors.add(app.router.add_resource(r""))
    # Worker registration, heartbeat, and deregistration are owned by the v3
    # protocol (see ``worker_v3.py``). The v2 module retains only the
    # read-only / admin endpoints — listing, single-worker inspection, and
    # the legacy token-lookup used by the conf middleware.
    cors.add(root_resource.add_route("GET", list_workers))
    cors.add(add_route("GET", "/{worker_id}", get_worker))
    cors.add(add_route("GET", "/{worker_id}/circuits", list_worker_circuits))
    return app, []
