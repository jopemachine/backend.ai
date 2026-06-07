"""Tests for the BackendKind enum and the runtime backfill heuristic in
:meth:`Worker.create`. The legacy ``WorkerMode`` enum was retired —
``BackendKind.worker_mode`` now derives the ``self-hosted`` /
``external-backend`` classification at the type level.
"""

from typing import Any
from uuid import uuid4

import pytest

from ai.backend.appproxy.common.types import (
    AppMode,
    BackendKind,
    FrontendMode,
    ProxyProtocol,
)
from ai.backend.appproxy.coordinator.models.worker import Worker


class TestBackendKindEnum:
    def test_values(self) -> None:
        assert BackendKind("native") is BackendKind.NATIVE
        assert BackendKind("traefik") is BackendKind.TRAEFIK
        assert BackendKind("continuum") is BackendKind.CONTINUUM
        assert set(BackendKind) == {
            BackendKind.NATIVE,
            BackendKind.TRAEFIK,
            BackendKind.CONTINUUM,
        }

    def test_worker_mode_derivation(self) -> None:
        assert BackendKind.NATIVE.worker_mode == "self-hosted"
        assert BackendKind.TRAEFIK.worker_mode == "external-backend"
        assert BackendKind.CONTINUUM.worker_mode == "external-backend"


class TestWorkerCreateBackfillHeuristic:
    """``Worker.create`` infers ``backend_kind`` from
    ``traefik_last_used_marker_path`` when the caller does not pass it
    explicitly. The Alembic migration uses the same heuristic when
    populating pre-existing rows.
    """

    @pytest.fixture
    def base_kwargs(self) -> dict[str, Any]:
        return {
            "id": uuid4(),
            "authority": "worker-1",
            "frontend_mode": FrontendMode.PORT,
            "protocol": ProxyProtocol.HTTP,
            "hostname": "localhost",
            "tls_listen": False,
            "tls_advertised": False,
            "api_port": 8080,
            "accepted_traffics": [AppMode.INFERENCE],
            "port_range": (10000, 10009),
        }

    def test_no_traefik_marker_yields_native(self, base_kwargs: dict[str, Any]) -> None:
        worker = Worker.create(**base_kwargs)
        assert worker.backend_kind is BackendKind.NATIVE
        assert worker.backend_kind.worker_mode == "self-hosted"

    def test_traefik_marker_yields_traefik(self, base_kwargs: dict[str, Any]) -> None:
        worker = Worker.create(
            **base_kwargs,
            traefik_last_used_marker_path="/run/appproxy-worker/last-used.sock",
        )
        assert worker.backend_kind is BackendKind.TRAEFIK
        assert worker.backend_kind.worker_mode == "external-backend"

    def test_explicit_backend_kind_wins(self, base_kwargs: dict[str, Any]) -> None:
        worker = Worker.create(**base_kwargs, backend_kind=BackendKind.CONTINUUM)
        assert worker.backend_kind is BackendKind.CONTINUUM
        assert worker.backend_kind.worker_mode == "external-backend"
