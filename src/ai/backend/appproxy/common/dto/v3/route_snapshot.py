"""
Pydantic v2 DTOs for the v3 route-snapshot polling protocol.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import Field

from ai.backend.common.api_handlers import BaseResponseModel

__all__ = (
    "EndpointDTO",
    "ReplicaDTO",
    "UnifiedRouteSnapshot",
)


class ReplicaDTO(BaseResponseModel):
    """A single replica backing an endpoint."""

    replica_id: UUID = Field(description="Stable identifier of the replica.")
    kernel_host: str = Field(description="Hostname or IP of the upstream kernel.")
    kernel_port: int = Field(
        ge=0,
        le=65535,
        description="TCP port of the upstream kernel.",
    )
    model_name: str = Field(description="Model served by this replica.")
    weight: float = Field(
        default=1.0,
        ge=0.0,
        description="Relative routing weight for load-balancing.",
    )


class EndpointDTO(BaseResponseModel):
    """A logical endpoint and its set of replicas."""

    endpoint_id: UUID = Field(description="Stable identifier of the endpoint.")
    app: str = Field(description="Application name advertised by the endpoint.")
    replicas: list[ReplicaDTO] = Field(description="Replicas serving this endpoint.")
    domain: str | None = Field(
        default=None,
        description="Fully-qualified domain the endpoint is exposed on, if any.",
    )
    port: int | None = Field(
        default=None,
        ge=0,
        le=65535,
        description="Public-facing port the endpoint is exposed on, if any.",
    )


class UnifiedRouteSnapshot(BaseResponseModel):
    """Backend-kind-agnostic route snapshot consumed by every worker.

    Workers receive this abstract shape (endpoints + replicas) and translate
    it into their own data-plane format (nghttpx / Traefik dynamic config /
    Continuum router config). Pre-translation on the Coordinator side
    (per-backend snapshot formats) is a future option once a worker-side
    adapter cost becomes load-bearing — for now the worker-side adapter
    handles it.
    """

    route_version: int = Field(
        ge=0,
        description=(
            "Content-hash identifier of the routing tree at snapshot time. "
            "Stable across calls with the same DB state — used by the "
            "Coordinator as the ETag value for If-None-Match short-circuit."
        ),
    )
    issued_at: datetime = Field(description="Timestamp at which the snapshot was issued.")
    endpoints: list[EndpointDTO] = Field(description="Endpoints included in the snapshot.")
