"""Wire-contract tests for the unified Worker ↔ Coordinator DTOs.

These tests pin the exact JSON shape that Continuum (Rust) and the
Python Worker send to the Coordinator. The fixtures replicate, byte
for byte, what each client emits — if a Pydantic field is renamed,
re-typed, or its default changes, these tests fail before any wire
drift can reach production.

The Rust side (``continuum-router/src/infrastructure/backends/
backend_ai_appproxy/worker_client.rs``) is the unverifiable peer; the
Python side validates against the same Pydantic models the Coordinator
uses on the request path.
"""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from ai.backend.appproxy.common.dto.worker_contract import (
    WorkerHeartbeatResponse,
    WorkerRegistrationRequest,
    WorkerRegistrationResponseV3,
    WorkerSelfReport,
)
from ai.backend.appproxy.common.types import (
    AppMode,
    BackendKind,
    FrontendMode,
    ProxyProtocol,
)


# ---------------------------------------------------------------------------
# Register request — payload Continuum-router (Rust) actually sends.
# ---------------------------------------------------------------------------
class TestRegisterRequestContinuumDefaults:
    """Continuum 의 ``continuum_defaults`` 가 보내는 JSON 정합."""

    PAYLOAD: dict[str, object] = {
        "authority": "ba-coord",
        "frontend_mode": "port",
        "protocol": "http",
        "hostname": "127.0.0.1",
        "tls_listen": False,
        "api_port": 18888,
        "accepted_traffics": ["inference"],
        "filtered_apps_only": False,
        "backend_kind": "continuum",
        "port_range": [18888, 18888],
        "capabilities": {
            "supports_v3_polling": True,
            "formats": ["continuum-native"],
        },
    }

    def test_parses_strictly(self) -> None:
        req = WorkerRegistrationRequest.model_validate(self.PAYLOAD)
        assert req.authority == "ba-coord"
        assert req.frontend_mode == FrontendMode.PORT
        assert req.protocol == ProxyProtocol.HTTP
        assert req.backend_kind == BackendKind.CONTINUUM
        assert req.backend_kind.worker_mode == "external-backend"
        assert req.port_range == (18888, 18888)
        assert req.accepted_traffics == [AppMode.INFERENCE]
        assert req.capabilities == {
            "supports_v3_polling": True,
            "formats": ["continuum-native"],
        }
        # Defaulted optional fields must NOT shift the wire — they exist
        # only as Pydantic defaults for fields Continuum omits.
        assert req.tls_advertised is False
        assert req.wildcard_domain is None
        assert req.scope is None
        assert req.app_filters == []


# ---------------------------------------------------------------------------
# Register request — payload Python Worker NATIVE actually sends.
# ---------------------------------------------------------------------------
class TestRegisterRequestPythonWorkerNative:
    """Python Worker NATIVE 가 ``_build_registration_request`` 로 보내는 JSON 정합."""

    PAYLOAD: dict[str, object] = {
        "authority": "worker-1",
        "frontend_mode": "port",
        "protocol": "http",
        "hostname": "127.0.0.1",
        "tls_listen": False,
        "tls_advertised": False,
        "api_port": 5050,
        "accepted_traffics": ["inference", "interactive"],
        "filtered_apps_only": False,
        "app_filters": [],
        "traefik_last_used_marker_path": None,
        "backend_kind": "native",
        "port_range": [10200, 10299],
        "capabilities": {"supports_v3_polling": False},
    }

    def test_parses_strictly(self) -> None:
        req = WorkerRegistrationRequest.model_validate(self.PAYLOAD)
        assert req.authority == "worker-1"
        assert req.backend_kind == BackendKind.NATIVE
        assert req.backend_kind.worker_mode == "self-hosted"
        assert req.port_range == (10200, 10299)
        assert req.accepted_traffics == [AppMode.INFERENCE, AppMode.INTERACTIVE]
        assert req.capabilities == {"supports_v3_polling": False}


# ---------------------------------------------------------------------------
# Heartbeat request — payload Continuum-router actually sends.
# ---------------------------------------------------------------------------
class TestHeartbeatRequest:
    """Continuum 이 매 polling 마다 보내는 ``WorkerSelfReport`` 정합."""

    PAYLOAD: dict[str, object] = {
        "current_route_version": 0,
        "polled_route_version": 0,
        "applied_route_version": 0,
        "last_poll_status_code": 200,
        "last_poll_timestamp": "2024-05-01T00:00:00Z",
        "subprocess_health": "running",
        "route_count": 0,
        "in_flight_requests": 0,
    }

    def test_parses_strictly(self) -> None:
        rep = WorkerSelfReport.model_validate(self.PAYLOAD)
        assert rep.current_route_version == 0
        assert rep.applied_route_version == 0
        assert rep.subprocess_health == "running"

    def test_invalid_subprocess_health_rejected(self) -> None:
        """Schema drift detection: a stray ``"healthy"`` MUST be rejected."""
        bad = dict(self.PAYLOAD)
        bad["subprocess_health"] = "healthy"
        try:
            WorkerSelfReport.model_validate(bad)
        except Exception as e:
            assert "Literal" in str(e) or "literal_error" in str(e)
        else:
            raise AssertionError("Expected Pydantic ValidationError for invalid Literal")


# ---------------------------------------------------------------------------
# Response models — coordinator emits these via ``model_dump(mode="json")``.
# ---------------------------------------------------------------------------
class TestResponseRoundtrip:
    """Pydantic ``model_dump(mode="json")`` 가 wire-compatible 한지 검증."""

    def test_register_response_v3_roundtrip(self) -> None:
        resp = WorkerRegistrationResponseV3(worker_id=uuid4())
        wire = resp.model_dump(mode="json")
        assert isinstance(wire["worker_id"], str)
        WorkerRegistrationResponseV3.model_validate(wire)

    def test_heartbeat_response_roundtrip(self) -> None:
        resp = WorkerHeartbeatResponse(
            ack_at=datetime.fromisoformat("2024-05-01T00:00:00+00:00"),
            poll_interval_hint_ms=5000,
        )
        wire = resp.model_dump(mode="json")
        assert "ack_at" in wire and "poll_interval_hint_ms" in wire
        WorkerHeartbeatResponse.model_validate(wire)
