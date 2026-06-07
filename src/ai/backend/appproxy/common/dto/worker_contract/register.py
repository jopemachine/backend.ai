"""Register request / response DTOs.

The legacy v2 endpoint ``PUT /api/worker`` and the v3 endpoint
``POST /api/v3/worker`` both accept :class:`WorkerRegistrationRequest`
on the wire. The response shape differs by endpoint — v2 returns a full
``WorkerResponseModel`` (the worker row with its slot list), v3 returns
just the assigned worker id (:class:`WorkerRegistrationResponseV3`) —
both response models live as siblings here. Coordinator handlers pick
the appropriate response model at the endpoint boundary; both call into
the same internal upsert.

Both v2 and v3 endpoints use the same ``X-BackendAI-Token`` (api_secret)
header for authentication on every call — there is no separate
poll-token issuance / refresh.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from pydantic import Field

from ai.backend.appproxy.common.types import (
    AppMode,
    BackendKind,
    FrontendMode,
    ProxyProtocol,
)
from ai.backend.common.api_handlers import BaseRequestModel, BaseResponseModel


class AppFilterEntry(BaseRequestModel):
    """One ``(key, value)`` filter that scopes a route to a worker."""

    key: Annotated[
        str, Field(description="App-filter key (matches WorkerAppFilter.property_name).")
    ]
    value: Annotated[
        str, Field(description="App-filter value (matches WorkerAppFilter.property_value).")
    ]


class WorkerRegistrationRequest(BaseRequestModel):
    """Wire-canonical register body for *every* Worker ↔ Coordinator track.

    The same payload is accepted by:

      * Legacy v2 ``PUT /api/worker`` — Python Worker (NATIVE / TRAEFIK).
      * v3 ``POST /api/v3/worker``    — Continuum Router (Rust).

    Both v2 and v3 use the same ``X-BackendAI-Token`` (api_secret) auth
    header. The two endpoints differ only in the response shape:
    see :class:`WorkerRegistrationResponseV3` and the legacy
    ``WorkerResponseModel`` returned by ``worker_v2.update_worker``.

    Fields that are NATIVE / TRAEFIK-only (``wildcard_domain``,
    ``traefik_last_used_marker_path``, ``app_filters``) default to safe
    values so the Continuum Router can omit them entirely. ``backend_kind``
    is optional — when omitted, the coordinator falls back to the
    traefik-marker heuristic and defaults to NATIVE.
    """

    # --- Core identity -----------------------------------------------------
    authority: Annotated[
        str,
        Field(
            description=(
                "Authority string the worker advertises to other workers and the manager. "
                "Unique across all workers attached to one coordinator."
            ),
        ),
    ]

    # --- Listen / dial geometry -------------------------------------------
    frontend_mode: Annotated[FrontendMode, Field(description="Listen pattern (port | wildcard).")]
    protocol: Annotated[
        ProxyProtocol, Field(description="Wire protocol the worker speaks (http | http2 | tcp).")
    ]
    hostname: Annotated[str, Field(description="Public host or IP the worker is reachable on.")]
    tls_listen: Annotated[
        bool, Field(description="True when the worker terminates TLS on its listen socket.")
    ]
    tls_advertised: Annotated[
        bool,
        Field(
            default=False,
            description="True when the externally-advertised URL is HTTPS (may differ from tls_listen when a TLS-terminating proxy sits in front).",
        ),
    ]
    api_port: Annotated[int, Field(description="API port the worker listens on (or advertises).")]

    # --- Port / wildcard subset -------------------------------------------
    port_range: Annotated[
        tuple[int, int] | None,
        Field(
            default=None,
            description="Inclusive (start, end) of ports the worker can hand to circuits. Required for PORT frontend; None otherwise.",
        ),
    ]
    wildcard_domain: Annotated[
        str | None,
        Field(
            default=None,
            description="Wildcard domain the worker accepts traffic on. Required for WILDCARD_DOMAIN frontend; None otherwise.",
        ),
    ]
    wildcard_traffic_port: Annotated[
        int | None,
        Field(default=None, description="Port that backs the wildcard-domain frontend."),
    ]

    # --- Traffic / filtering ----------------------------------------------
    accepted_traffics: Annotated[
        list[AppMode],
        Field(
            default_factory=lambda: [AppMode.INFERENCE],
            description="App modes the worker is willing to serve (inference | interactive).",
        ),
    ]
    filtered_apps_only: Annotated[
        bool,
        Field(
            default=False,
            description="True restricts traffic to apps that match an entry in ``app_filters``.",
        ),
    ]
    app_filters: Annotated[
        list[AppFilterEntry],
        Field(
            default_factory=list,
            description="(key, value) entries scoping which apps may land on this worker.",
        ),
    ]
    traefik_last_used_marker_path: Annotated[
        str | None,
        Field(
            default=None,
            description="UDS path the Traefik subprocess writes last-used markers to. TRAEFIK workers only; None otherwise.",
        ),
    ]

    # --- Backend kind and capability advertisement -----------------------
    backend_kind: Annotated[
        BackendKind | None,
        Field(
            default=None,
            description=(
                "BackendKind the worker drives (``native`` | ``traefik`` | ``continuum``). Older workers omit this."
            ),
        ),
    ]
    capabilities: Annotated[
        dict[str, Any] | None,
        Field(
            default=None,
            description=(
                "Capability dict the worker advertises. ``supports_v3_polling=true`` flips the coordinator "
                "to STARTING-until-first-apply cold-start gating; ``formats`` lists the snapshot formats "
                "the worker can consume."
            ),
        ),
    ]
    scope: Annotated[
        dict[str, Any] | None,
        Field(
            default=None,
            description=(
                "Optional scope filter (project_ids / scaling_group / backend_kinds) the worker handles. "
                "Empty / None means no filter."
            ),
        ),
    ]


class WorkerRegistrationResponseV3(BaseResponseModel):
    """v3 register response (``POST /api/v3/worker``).

    The coordinator persists the worker and returns its assigned id.
    Every subsequent v3 call uses the same ``X-BackendAI-Token``
    (api_secret) header as v2 — there is no separate poll-token issuance.
    """

    worker_id: Annotated[UUID, Field(description="Coordinator-assigned worker UUID.")]
