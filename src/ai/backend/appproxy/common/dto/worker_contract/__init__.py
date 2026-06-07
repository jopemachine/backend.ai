"""Unified wire contract for Worker ↔ Coordinator communication.

This package is the **single source of truth** for the JSON shape spoken
over every Worker ↔ Coordinator endpoint, both the legacy v2 routes
(``PUT /api/worker``, ``PATCH /api/worker/{id}``) used by the Python
NATIVE / TRAEFIK workers and the v3 polling routes
(``POST /api/v3/worker``, ``PATCH /api/v3/worker/{id}/heartbeat``,
``GET /api/v3/worker/{id}/routes``) spoken by the Continuum Router binary.

Both Python (Coordinator handlers, Python Worker client) and Rust
(``continuum-router/src/infrastructure/backends/backend_ai_appproxy/``)
implementations target the same shapes defined here. Drift between the
languages is caught at integration test time
(``tests/integration/appproxy/test_v3_wire_contracts.py``); there is no
generated stub layer — Rust developers are expected to keep
``worker_client.rs`` in lockstep with the Pydantic models, and CI is the
enforcement mechanism.

Pydantic is the canonical schema. If a model lives here, it must be
Pydantic (no ``dict`` literals on the wire). Coordinator handlers MUST
use ``model_validate`` on input and emit ``model_dump_json()`` for
output — no ``body.get(...)`` and no ``web.json_response({"foo": ...})``
for any field defined in this module.
"""

from __future__ import annotations

from ai.backend.appproxy.common.dto.v3.route_snapshot import (
    SubprocessHealth,
    WorkerCapabilities,
    WorkerScope,
    WorkerSelfReport,
)

from .heartbeat import WorkerHeartbeatResponse
from .register import (
    WorkerRegistrationRequest,
    WorkerRegistrationResponseV3,
)

__all__ = (
    # Register
    "WorkerRegistrationRequest",
    "WorkerRegistrationResponseV3",
    # Heartbeat
    "WorkerHeartbeatResponse",
    "WorkerSelfReport",
    "SubprocessHealth",
    # Re-exports from v3/route_snapshot.py (kept canonical here)
    "WorkerCapabilities",
    "WorkerScope",
)
