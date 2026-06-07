"""Tests for the OB-COMP-4 mitigation in ``pick_worker``.

The ``_is_polling_ready`` predicate gates v3-polling workers on
``committed_route_version > 0`` so the coordinator never picks a worker
whose Continuum data-plane has not yet applied its first snapshot. Legacy
NATIVE / SELF_HOSTED workers never call the v3 polling endpoints, so they
are intentionally exempt — otherwise the seed value of
``committed_route_version = 0`` would permanently exclude every legacy
worker from circuit assignment (silent regression).

These cases exercise the predicate directly so the regression cannot
silently come back via a refactor of ``pick_worker``.
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
from ai.backend.appproxy.coordinator.models.worker import (
    Worker,
    _is_polling_ready,
)


def _make_worker(
    *,
    backend_kind: BackendKind,
    capabilities: dict[str, Any],
    committed_route_version: int,
) -> Worker:
    """Build a Worker row with the minimum fields needed for the predicate.

    ``_is_polling_ready`` only inspects ``capabilities`` and
    ``committed_route_version``; the remaining fields are set so the row
    is internally consistent but otherwise irrelevant to the predicate.
    """
    worker = Worker.create(
        id=uuid4(),
        authority="worker-under-test",
        frontend_mode=FrontendMode.PORT,
        protocol=ProxyProtocol.HTTP,
        hostname="localhost",
        tls_listen=False,
        tls_advertised=False,
        api_port=8080,
        accepted_traffics=[AppMode.INFERENCE],
        port_range=(10000, 10009),
        backend_kind=backend_kind,
    )
    worker.capabilities = capabilities
    worker.committed_route_version = committed_route_version
    return worker


class TestIsPollingReadyCompFour:
    """OB-COMP-4: ``committed_route_version == 0`` must NOT exclude NATIVE
    workers from ``pick_worker``'s candidate set.
    """

    def test_native_worker_with_zero_route_version_is_included(self) -> None:
        """Case 1: NATIVE worker, ``committed_route_version=0`` -> included.

        Legacy NATIVE workers never poll, so their committed version is
        permanently 0. The predicate MUST treat them as polling-ready.
        """
        worker = _make_worker(
            backend_kind=BackendKind.NATIVE,
            capabilities={},
            committed_route_version=0,
        )

        assert _is_polling_ready(worker) is True

    def test_v3_polling_worker_with_zero_route_version_is_excluded(self) -> None:
        """Case 2: v3-polling worker, ``committed_route_version=0`` -> excluded.

        The worker declared the capability but has not yet applied a
        snapshot; routing to it now would land traffic on an empty
        data-plane. The predicate MUST exclude it from the candidate set.
        """
        worker = _make_worker(
            backend_kind=BackendKind.CONTINUUM,
            capabilities={"supports_v3_polling": True},
            committed_route_version=0,
        )

        assert _is_polling_ready(worker) is False

    def test_v3_polling_worker_with_committed_route_version_is_included(self) -> None:
        """Case 3: v3-polling worker, ``committed_route_version=42`` -> included.

        The worker has reported at least one successfully applied snapshot,
        so its data-plane is now authoritative and it is eligible to serve
        new circuits.
        """
        worker = _make_worker(
            backend_kind=BackendKind.CONTINUUM,
            capabilities={"supports_v3_polling": True},
            committed_route_version=42,
        )

        assert _is_polling_ready(worker) is True

    @pytest.mark.parametrize(
        "falsy_capabilities",
        [{}, {"supports_v3_polling": False}, {"other_flag": True}],
    )
    def test_falsy_capabilities_treated_as_legacy_native(
        self, falsy_capabilities: dict[str, Any]
    ) -> None:
        """Workers whose capabilities do NOT declare ``supports_v3_polling``
        (missing key or falsy value) must be treated as legacy and bypass
        the freshness gate.
        """
        worker = _make_worker(
            backend_kind=BackendKind.NATIVE,
            capabilities=falsy_capabilities,
            committed_route_version=0,
        )

        assert _is_polling_ready(worker) is True
