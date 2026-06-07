"""
Pydantic v2 DTOs for the v3 route-snapshot polling protocol.

SEC-4 (Secret Handling Boundary)
================================
These DTOs intentionally NEVER embed secret material (API keys, bearer tokens,
upstream credentials, signed URLs, or any other bytes whose disclosure would
grant access to a backing model service). Replicas that require credentials
expose only an opaque ``credential_ref`` — a stable identifier that the worker
resolves against a separate, audited secrets endpoint right before issuing
the upstream request.

Rationale:

* Route snapshots are pulled on a fixed poll interval and may be cached
  in worker memory, persisted to local disk (for crash-restart replay), and
  inspected via debug/admin tooling. A secret leaking into any of those
  surfaces would be very hard to revoke.
* Snapshots are also a frequent target of structured logging. Keeping them
  free of secrets means we can log full payloads without redaction filters.
* Decoupling secret resolution from route distribution lets us rotate
  credentials without bumping ``route_version`` and re-fanning the snapshot
  to every worker.

Any future field added to this module MUST satisfy the same rule: opaque
references only, never the secret value itself.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import Field

from ai.backend.common.api_handlers import BaseResponseModel

__all__ = (
    "BackendType",
    "ContinuumNativeSnapshot",
    "EndpointDTO",
    "ReplicaDTO",
    "SubprocessHealth",
    "TraefikNativeSnapshot",
    "UnifiedRouteSnapshot",
    "WorkerCapabilities",
    "WorkerScope",
    "WorkerSelfReport",
)


BackendType = Literal[
    "ba_kernel",
    "external_openai",
    "external_anthropic",
    "external_bedrock",
    "external_other",
]


SubprocessHealth = Literal[
    "starting",
    "running",
    "crashed",
    "draining",
]


# 5 MiB — conservative ceiling that fits typical route fan-outs while
# bounding worst-case memory use per poll.
_DEFAULT_MAX_PAYLOAD_BYTES: int = 5 * 1024 * 1024


class WorkerCapabilities(BaseResponseModel):
    """Capability advertisement a worker sends when registering for v3 polling."""

    supports_v3_polling: bool = Field(
        description="Whether the worker speaks the v3 polling protocol.",
    )
    formats: list[str] = Field(
        description=(
            "Snapshot formats the worker can consume "
            "(e.g. ``unified``, ``traefik_native``, ``continuum_native``)."
        ),
    )
    poll_interval_ms: int = Field(
        default=5000,
        ge=0,
        description="Desired interval between successive polls, in milliseconds.",
    )
    max_payload_bytes: int = Field(
        default=_DEFAULT_MAX_PAYLOAD_BYTES,
        ge=0,
        description="Maximum snapshot payload size the worker is willing to accept, in bytes.",
    )


class WorkerScope(BaseResponseModel):
    """Filter declaring which subset of routes a worker is responsible for."""

    project_ids: list[UUID] | None = Field(
        default=None,
        description="Project UUIDs the worker serves. ``None`` means no project filter.",
    )
    scaling_group: str | None = Field(
        default=None,
        description="Scaling group the worker is attached to. ``None`` means no scaling-group filter.",
    )
    backend_kinds: list[str] | None = Field(
        default=None,
        description="Backend kinds (e.g. ``traefik``, ``continuum``) the worker handles. "
        "``None`` means no backend-kind filter.",
    )


class ReplicaDTO(BaseResponseModel):
    """A single replica backing an endpoint.

    SEC-4 / OB-COMP-1: ``credential_ref`` is an opaque reference resolved
    via the dedicated secrets endpoint (``GET /api/v3/worker/{worker_id}/
    secrets/{ref}``) immediately before upstream dispatch; the secret
    value itself is never embedded in this DTO.

    URI shape of ``credential_ref``:

      * ``secret://worker/{wid}/inference/{deployment_id}`` — per-deployment
        inference JWT for external LLM backends (e.g. ``external_openai``,
        ``external_anthropic``, ``external_bedrock``). The worker fetches
        the raw bearer token via the secrets endpoint immediately before
        signing the upstream request.
      * ``None`` — for ``ba_kernel`` replicas: Backend.AI's own service
        token issued elsewhere already covers upstream authentication, so
        no separate credential lookup is required.
    """

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
    backend_type: BackendType = Field(
        default="ba_kernel",
        description="Backend kind that fulfils requests for this replica.",
    )
    credential_ref: str | None = Field(
        default=None,
        description=(
            "Opaque reference to credential material held by the secrets service. "
            "Resolved out-of-band per SEC-4 / OB-COMP-1; the secret value is "
            "never embedded. "
            "Example: ``secret://worker/{wid}/inference/{deployment_id}`` for "
            "external LLM backends; ``None`` for ba_kernel replicas where "
            "Backend.AI's own service token already covers upstream auth."
        ),
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
    """Format-agnostic route snapshot consumed by every worker backend."""

    route_version: int = Field(
        ge=0,
        description="Monotonically increasing version identifying this snapshot.",
    )
    issued_at: datetime = Field(description="Timestamp at which the snapshot was issued.")
    scope_fingerprint: str = Field(
        description="Stable hash of the scope filter used to produce this snapshot.",
    )
    endpoints: list[EndpointDTO] = Field(description="Endpoints included in the snapshot.")


class TraefikNativeSnapshot(BaseResponseModel):
    """Traefik-native snapshot variant.

    TODO: populate ``http`` with the concrete ``routers``/``services``/
    ``middlewares`` schemas once the Traefik adapter contract is finalised.
    """

    route_version: int = Field(
        ge=0,
        description="Monotonically increasing version identifying this snapshot.",
    )
    issued_at: datetime = Field(description="Timestamp at which the snapshot was issued.")
    http: dict[str, Any] = Field(
        default_factory=dict,
        description="Traefik dynamic HTTP configuration (routers/services/middlewares).",
    )


class ContinuumNativeSnapshot(BaseResponseModel):
    """Continuum-router-native snapshot variant.

    ``backends`` and ``models`` are stubbed as opaque dicts pending the final
    Continuum schema.
    """

    route_version: int = Field(
        ge=0,
        description="Monotonically increasing version identifying this snapshot.",
    )
    issued_at: datetime = Field(description="Timestamp at which the snapshot was issued.")
    backends: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Continuum backend definitions (schema TBD).",
    )
    models: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Continuum model definitions (schema TBD).",
    )


class WorkerSelfReport(BaseResponseModel):
    """Health/state self-report a worker attaches to each poll."""

    current_route_version: int = Field(
        ge=0,
        description="Route version currently in effect on the worker.",
    )
    polled_route_version: int = Field(
        ge=0,
        description="Route version observed in the most recent poll response.",
    )
    applied_route_version: int = Field(
        ge=0,
        description="Route version successfully applied to the subprocess.",
    )
    last_poll_status_code: int = Field(
        description="HTTP status code returned by the most recent poll.",
    )
    last_poll_timestamp: datetime = Field(
        description="Timestamp of the most recent poll attempt.",
    )
    subprocess_health: SubprocessHealth = Field(
        description="Current lifecycle state of the proxy subprocess.",
    )
    route_count: int = Field(
        ge=0,
        description="Number of routes currently programmed into the subprocess.",
    )
    in_flight_requests: int = Field(
        ge=0,
        description="Number of requests currently in flight on the worker.",
    )
