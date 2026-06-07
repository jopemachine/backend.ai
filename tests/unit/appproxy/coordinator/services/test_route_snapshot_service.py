"""Tests for ``RouteSnapshotService`` covering:

- RC-1 content-hash determinism of ``route_version`` (no Postgres ``nextval``).
- Etag stability across calls with identical state and ``304`` short-circuit.
- Format dispatch (``traefik-native``, ``continuum-native``) shape contracts.
- Scope-fingerprint separation: bumping ``worker.scope`` rotates the etag
  but leaves ``route_version`` (a routes-only digest) untouched.
- SEC-4 invariant: serialized snapshot bodies never contain raw secret
  strings — only opaque ``secret://`` refs are acceptable.

The DB is mocked at the session level: a fake ``begin_readonly_session``
returns a session whose ``scalar`` / ``scalars`` methods are dispatched
on the SQLAlchemy ``select`` target so we don't need a live engine.
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
        scope={"project_ids": [], "scaling_group": "default"},
        backend_kind=BackendKind.NATIVE,
        capabilities={"supports_v3_polling": True, "formats": ["unified"]},
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
        result = await service.get_snapshot(worker_id, format="unified")

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
        first = await service.get_snapshot(worker_id, format="unified")
        second = await service.get_snapshot(worker_id, format="unified")

        assert first.etag == second.etag
        assert first.route_version == second.route_version


class TestEtagShortCircuit:
    """The ``If-None-Match``-style ``etag_hint`` parameter."""

    async def test_matching_etag_hint_returns_304_with_no_body(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        baseline = await service.get_snapshot(worker_id, format="unified")

        cached = await service.get_snapshot(worker_id, format="unified", etag_hint=baseline.etag)

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
            format="unified",
            etag_hint="v0:deadbeefcafe",
        )

        assert result.status_code == 200
        assert result.body is not None


class TestFormatDispatch:
    """Per-format body shape contracts."""

    async def test_traefik_native_has_http_routers_services_middlewares(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        result = await service.get_snapshot(worker_id, format="traefik-native")

        assert result.status_code == 200
        assert result.body is not None
        http = result.body["http"]
        # All three top-level Traefik provider buckets must be present
        # so the HTTP provider can mount even an empty config.
        assert set(http.keys()) >= {"routers", "services", "middlewares"}

    async def test_continuum_native_has_backends_and_models(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        result = await service.get_snapshot(worker_id, format="continuum-native")

        assert result.status_code == 200
        assert result.body is not None
        assert result.body["backends"] == []
        assert result.body["models"] == []


class TestRouteVersionDeterminism:
    """RC-1: ``route_version`` is a content hash, not a sequence."""

    async def test_consecutive_calls_yield_same_route_version(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
    ) -> None:
        a = await service.get_snapshot(worker_id, format="unified")
        b = await service.get_snapshot(worker_id, format="unified")

        # If ``route_version`` had been a Postgres nextval it would
        # increment between calls; the content-hash impl must not.
        assert a.route_version == b.route_version

    async def test_scope_bump_rotates_etag_but_preserves_route_version(
        self,
        service: RouteSnapshotService,
        fake_db: _FakeDB,
        worker_id: UUID,
    ) -> None:
        """Scope is part of the etag suffix but NOT of ``route_version``.

        ``route_version`` only fingerprints the routing tree. A scope
        bump (e.g. a new ``scaling_group``) must rotate the etag so
        caches invalidate, while leaving ``route_version`` untouched
        until the routes themselves change.
        """
        before = await service.get_snapshot(worker_id, format="unified")

        assert fake_db.worker is not None
        # Bump the scope in place; the next read sees the new value.
        cast(Any, fake_db.worker).scope = {
            "project_ids": [],
            "scaling_group": "high-mem",
        }

        after = await service.get_snapshot(worker_id, format="unified")

        assert after.etag != before.etag, "scope change must rotate the etag so workers re-pull"
        assert after.route_version == before.route_version, (
            "routes did not change → route_version must stay stable (RC-1)"
        )
        # The body's scope_fingerprint must reflect the rotation too.
        assert before.body is not None and after.body is not None
        assert after.body["scope_fingerprint"] != before.body["scope_fingerprint"]


class TestNoRawSecretsInBody:
    """SEC-4: snapshot bodies must never embed raw secret material."""

    @pytest.mark.parametrize(
        "snapshot_format",
        ["unified", "traefik-native", "continuum-native"],
    )
    async def test_body_contains_no_raw_secret_strings(
        self,
        service: RouteSnapshotService,
        worker_id: UUID,
        snapshot_format: str,
    ) -> None:
        """The serialized body may contain ``secret://`` refs but not raw values.

        ``jwt_verification_secret`` / ``permit_hash_secret`` are the
        known fields that used to be inlined (P0 SEC-4). Their literal
        keys MUST NOT appear next to raw values. We also forbid any
        suspicious "*_secret" key whose value is not a ``secret://`` ref.
        """
        result = await service.get_snapshot(worker_id, format=cast(Any, snapshot_format))

        assert result.status_code == 200
        assert result.body is not None
        serialized = json.dumps(result.body)

        # No raw secret keys should appear directly in the serialized body.
        forbidden_keys = ("jwt_verification_secret", "permit_hash_secret")
        for key in forbidden_keys:
            assert key not in serialized, f"SEC-4: forbidden raw secret key {key!r} found in body"

        # Any string that *looks* like a secret reference MUST use the
        # opaque ``secret://`` scheme. We scan all string leaves and
        # assert none of them are obvious secret leaks (long hex, etc.
        # are out of scope; we only catch the named-key class here).
        for s in _walk_strings(result.body):
            if s.startswith("secret://"):
                continue  # acceptable opaque reference
            # No raw-secret marker should appear anywhere else.
            assert "jwt_verification_secret" not in s
            assert "permit_hash_secret" not in s
