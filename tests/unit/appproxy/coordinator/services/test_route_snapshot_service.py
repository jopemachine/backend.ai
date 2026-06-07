"""Tests for ``RouteSnapshotService`` covering:

- RC-1 content-hash determinism of ``route_version`` (no Postgres ``nextval``).
- Etag stability across calls with identical state and ``304`` short-circuit.
- Unified format body shape contract.

The DB is mocked at the session level: a fake ``begin_readonly_session``
returns a session whose ``scalar`` / ``scalars`` methods are dispatched
on the SQLAlchemy ``select`` target so we don't need a live engine.
"""

from __future__ import annotations

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
    RouteSnapshotResult,
    RouteSnapshotService,
)
from ai.backend.common.clients.valkey_client.valkey_live.client import ValkeyLiveClient

# ---- Fake DB infrastructure ---------------------------------------------------


class _FakeReadonlySession:
    """Stand-in for an ``AsyncSession`` opened by ``begin_readonly_session``.

    The snapshot service issues exactly two kinds of queries:
        * ``sess.scalar(sa.select(Worker).where(...))`` → returns the
          single ``Worker`` row (or ``None``).
        * ``sess.scalars(sa.select(Circuit).where(...).options(...))`` →
          returns a scalars result whose ``.all()`` is the list of
          ``Circuit`` rows.

    We dispatch on the select statement's column descriptions to know
    which collection to hand back without depending on the real
    SQLAlchemy execution machinery.
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
        # ``column_descriptions[0]["entity"]`` is the mapped class for
        # an ``sa.select(SomeModel)`` statement.
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
    """Implements just enough of ``ExtendedAsyncSAEngine`` for the service.

    The service only uses ``begin_readonly_session()`` as an async
    context manager; we expose the underlying state so individual tests
    can swap workers/circuits between calls to simulate routing changes.
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
    """A minimally populated ``Worker`` row.

    ``MagicMock(spec=Worker)`` is intentionally avoided here because
    the service reads attributes that mocks would auto-create as
    further mocks and break canonical-JSON serialization. A real
    ``SimpleNamespace`` keeps every field a primitive.
    """
    obj: Any = SimpleNamespace(
        id=worker_id,
        authority="worker-1",
        backend_kind=BackendKind.NATIVE,
    )
    return cast(Worker, obj)


@pytest.fixture
def fake_db(worker: Worker) -> _FakeDB:
    return _FakeDB(worker=worker, circuits=[])


@pytest.fixture
def valkey() -> ValkeyLiveClient:
    # The service tolerates cache failures, but a happy-path AsyncMock
    # avoids noisy debug logs and exercises the success branch.
    client: Any = AsyncMock(spec=ValkeyLiveClient)
    client.store_live_data = AsyncMock(return_value=None)
    return cast(ValkeyLiveClient, client)


@pytest.fixture
def service(fake_db: _FakeDB, valkey: ValkeyLiveClient) -> RouteSnapshotService:
    return RouteSnapshotService(cast(ExtendedAsyncSAEngine, fake_db), valkey)


# ---- Tests ------------------------------------------------------------------


class TestGetSnapshotEmptyState:
    """Scenarios with no circuits / endpoints registered."""

    async def test_empty_db_returns_200_with_empty_endpoints(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        """An empty routing tree still produces a valid ``200`` snapshot.

        The unified body must expose an empty ``endpoints`` list so the
        worker can distinguish "no routes" from "missing field".
        """
        result = await service.get_snapshot(worker_id)

        assert isinstance(result, RouteSnapshotResult)
        assert result.status_code == 200
        assert result.body is not None
        assert result.body["endpoints"] == []
        assert result.etag  # non-empty
        assert result.route_version >= 0

    async def test_etag_stable_across_two_calls_same_state(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        """RC-1: identical state → identical etag (no ``nextval`` drift)."""
        first = await service.get_snapshot(worker_id)
        second = await service.get_snapshot(worker_id)

        assert first.etag == second.etag
        assert first.route_version == second.route_version


class TestEtagShortCircuit:
    """The ``If-None-Match``-style ``etag_hint`` parameter."""

    async def test_matching_etag_hint_returns_304_with_no_body(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        baseline = await service.get_snapshot(worker_id)

        cached = await service.get_snapshot(worker_id, etag_hint=baseline.etag)

        assert cached.status_code == 304
        assert cached.body is None
        # Even on 304 the version is reported so the worker can update
        # its local bookkeeping.
        assert cached.etag == baseline.etag
        assert cached.route_version == baseline.route_version

    async def test_mismatching_etag_hint_returns_200_with_body(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        result = await service.get_snapshot(
            worker_id,
            etag_hint="vDEADBEEF",
        )

        assert result.status_code == 200
        assert result.body is not None


class TestRouteVersionDeterminism:
    """RC-1: ``route_version`` is a content hash, not a sequence."""

    async def test_consecutive_calls_yield_same_route_version(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        a = await service.get_snapshot(worker_id)
        b = await service.get_snapshot(worker_id)

        # If ``route_version`` had been a Postgres nextval it would
        # increment between calls; the content-hash impl must not.
        assert a.route_version == b.route_version
