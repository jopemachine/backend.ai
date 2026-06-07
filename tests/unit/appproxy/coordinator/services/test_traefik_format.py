"""Tests for ``RouteSnapshotService._format_traefik_native`` (OB-COMP-3).

The Traefik native formatter must emit a payload whose top-level shape
covers parity with every worker-side frontend kind:

* HTTP port-based (``TraefikPortFrontend``)
* HTTP subdomain-based (``TraefikSubdomainFrontend``) — modelled as
  Traefik HTTP routers with ``Host(...)`` rules, so it lives under the
  same ``http`` bucket as the port frontend.
* TCP (``TraefikTCPFrontend``) — modelled as Traefik ``tcpRouters`` /
  ``tcpServices``, exposed here under a top-level ``tcp`` bucket.

These tests are intentionally *shape-only* — the per-circuit content is
deferred behind ``TODO(BA-6068)`` markers in the formatter. The point of
the test is to lock in the structural contract so a downstream Traefik
instance reloading the snapshot doesn't 5xx on a missing top-level key
(OB-COMP-3), and to re-assert the SEC-4 / COMP-3 invariant that no raw
secret material is embedded in the snapshot body.

The DB is faked at the session level (same approach as
``test_route_snapshot_service.py``) so this test stays a pure unit test
with no engine dependency.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from ai.backend.appproxy.common.types import BackendKind
from ai.backend.appproxy.coordinator.models import Circuit, Worker
from ai.backend.appproxy.coordinator.models.utils import ExtendedAsyncSAEngine
from ai.backend.appproxy.coordinator.services.route_snapshot import (
    RouteSnapshotService,
)
from ai.backend.common.clients.valkey_client.valkey_live.client import ValkeyLiveClient

# ---- Fake DB infrastructure -------------------------------------------------


class _FakeReadonlySession:
    """Stand-in for ``AsyncSession`` returned by ``begin_readonly_session``.

    Only the two queries the snapshot service issues are modelled:

    * ``sess.scalar(sa.select(Worker).where(...))``
    * ``sess.scalars(sa.select(Circuit).where(...).options(...))``

    Dispatch is on the SQLAlchemy ``select`` target name so no real
    engine is needed.
    """

    def __init__(
        self,
        *,
        worker: Worker | None,
        circuits: list[Circuit],
    ) -> None:
        self._worker = worker
        self._circuits = circuits

    @staticmethod
    def _target_name(stmt: Any) -> str:
        try:
            name: str = stmt.column_descriptions[0]["entity"].__name__
            return name
        except Exception:  # pragma: no cover - defensive
            return ""

    async def scalar(self, stmt: Any) -> Any:
        if self._target_name(stmt) == "Worker":
            return self._worker
        raise AssertionError(f"unexpected scalar() target: {stmt!r}")

    async def scalars(self, stmt: Any) -> Any:
        if self._target_name(stmt) == "Circuit":
            return SimpleNamespace(all=lambda: list(self._circuits))
        raise AssertionError(f"unexpected scalars() target: {stmt!r}")


class _FakeDB:
    """Minimal ``ExtendedAsyncSAEngine`` substitute.

    Only ``begin_readonly_session()`` is exercised by the snapshot
    service, so that is the only surface we model.
    """

    def __init__(
        self,
        *,
        worker: Worker | None,
        circuits: list[Circuit] | None = None,
    ) -> None:
        self.worker = worker
        self.circuits: list[Circuit] = list(circuits or [])

    @asynccontextmanager
    async def begin_readonly_session(self) -> AsyncIterator[_FakeReadonlySession]:
        yield _FakeReadonlySession(worker=self.worker, circuits=self.circuits)


# ---- Fixtures ---------------------------------------------------------------


@pytest.fixture
def worker_id() -> UUID:
    return uuid4()


@pytest.fixture
def worker(worker_id: UUID) -> Worker:
    """A primitive-only ``Worker`` stand-in.

    ``SimpleNamespace`` is used (not ``MagicMock``) so the canonical
    JSON serializer in the snapshot service does not stumble over
    auto-generated mock attributes.
    """
    obj: Any = SimpleNamespace(
        id=worker_id,
        authority="worker-traefik-parity",
        scope={"project_ids": [], "scaling_group": "default"},
        # TRAEFIK backend kind pairs with EXTERNAL_BACKEND mode per the
        # BackendKind docstring in appproxy.common.types — the worker is
        # a control-loop that programs a Traefik instance.
        backend_kind=BackendKind.TRAEFIK,
        capabilities={"supports_v3_polling": True, "formats": ["traefik-native"]},
    )
    return cast(Worker, obj)


@pytest.fixture
def fake_db(worker: Worker) -> _FakeDB:
    return _FakeDB(worker=worker, circuits=[])


@pytest.fixture
def valkey() -> ValkeyLiveClient:
    """A happy-path cache stub — the service tolerates failures but we
    want to exercise the success branch for noise-free output."""
    client: Any = AsyncMock(spec=ValkeyLiveClient)
    client.store_live_data = AsyncMock(return_value=None)
    return cast(ValkeyLiveClient, client)


@pytest.fixture
def service(fake_db: _FakeDB, valkey: ValkeyLiveClient) -> RouteSnapshotService:
    return RouteSnapshotService(cast(ExtendedAsyncSAEngine, fake_db), valkey)


# ---- Helpers ----------------------------------------------------------------


def _walk_strings(value: Any) -> list[str]:
    """Flatten every string leaf of a JSON-serializable value."""
    out: list[str] = []
    if isinstance(value, str):
        out.append(value)
    elif isinstance(value, dict):
        for v in value.values():
            out.extend(_walk_strings(v))
    elif isinstance(value, list):
        for v in value:
            out.extend(_walk_strings(v))
    return out


# ---- Tests ------------------------------------------------------------------


class TestTraefikNativeTopLevelShape:
    """OB-COMP-3: parity for every frontend kind requires both buckets.

    The worker-side ``TraefikPortFrontend`` and
    ``TraefikSubdomainFrontend`` both consume the ``http`` bucket; the
    ``TraefikTCPFrontend`` consumes the ``tcp`` bucket. The snapshot
    must expose both unconditionally so a Traefik instance reloading
    the dynamic config never trips over a missing key.
    """

    async def test_body_has_both_http_and_tcp_top_level_keys(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        result = await service.get_snapshot(worker_id, format="traefik-native")

        assert result.status_code == 200
        assert result.body is not None
        # Both top-level provider buckets MUST be present.
        assert "http" in result.body, "missing 'http' bucket (port + subdomain frontends)"
        assert "tcp" in result.body, "missing 'tcp' bucket (TCP frontend)"

    async def test_http_bucket_has_routers_services_minimum(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        """HTTP bucket must carry the ``routers`` and ``services`` keys.

        ``middlewares`` is also expected by the worker frontend, but
        the formal contract this test pins down — and the minimum the
        downstream Traefik HTTP provider needs to mount the config —
        is ``routers`` + ``services``.
        """
        result = await service.get_snapshot(worker_id, format="traefik-native")

        assert result.body is not None
        http = result.body["http"]
        assert isinstance(http, dict), "'http' bucket must be a JSON object"
        assert "routers" in http, "'http.routers' is required"
        assert "services" in http, "'http.services' is required"

    async def test_tcp_bucket_has_routers_services_minimum(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        """TCP bucket must carry ``routers`` and ``services`` keys.

        These map to Traefik's ``tcpRouters`` / ``tcpServices``
        dynamic-config keys and back the worker's
        ``TraefikTCPFrontend`` consumer.
        """
        result = await service.get_snapshot(worker_id, format="traefik-native")

        assert result.body is not None
        tcp = result.body["tcp"]
        assert isinstance(tcp, dict), "'tcp' bucket must be a JSON object"
        assert "routers" in tcp, "'tcp.routers' is required"
        assert "services" in tcp, "'tcp.services' is required"


class TestTraefikNativeNoInlineSecrets:
    """SEC-4 + OB-COMP-3 alignment: no raw secret material in the body.

    Even when the per-circuit routers/services/middlewares grow real
    content, the secrets used by the permit-hash / JWT middlewares MUST
    be exposed as opaque ``secret://`` refs only. This test pins down
    the invariant for the *empty-skeleton* case shipped with this
    mitigation so the structure cannot regress into inlining secrets
    later.
    """

    async def test_no_known_raw_secret_keys_in_serialized_body(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        result = await service.get_snapshot(worker_id, format="traefik-native")

        assert result.body is not None
        serialized = json.dumps(result.body)

        # The two field names that historically leaked as inline values
        # (P0 SEC-4). Either must not appear at all, or — once routers
        # are populated — they MUST appear only with ``secret://`` ref
        # values, never raw material.
        for forbidden in ("jwt_verification_secret", "permit_hash_secret"):
            assert forbidden not in serialized, (
                f"SEC-4/COMP-3: forbidden raw secret key {forbidden!r} found in body"
            )

    async def test_every_string_leaf_is_either_safe_or_secret_ref(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        """Belt-and-suspenders: walk every string and assert none of
        them are obvious secret leaks. ``secret://`` refs are the only
        acceptable secret-shaped value."""
        result = await service.get_snapshot(worker_id, format="traefik-native")

        assert result.body is not None
        for leaf in _walk_strings(result.body):
            if leaf.startswith("secret://"):
                continue
            assert "jwt_verification_secret" not in leaf
            assert "permit_hash_secret" not in leaf
