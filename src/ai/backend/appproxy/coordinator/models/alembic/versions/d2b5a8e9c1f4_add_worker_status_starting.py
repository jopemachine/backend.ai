"""add WorkerStatus.STARTING value (OPS-2 mitigation)

Revision ID: d2b5a8e9c1f4
Revises: c1a4f7b2e8d3
Create Date: 2026-06-06

OPS-2 mitigation — Continuum cold-start gap:
    v3-polling workers run an out-of-band proxy subprocess (Continuum or
    Traefik) which must apply its first snapshot before it can carry
    traffic. The coordinator now registers such workers in the new
    ``STARTING`` status; only the v3 heartbeat path promotes them to
    ``ALIVE`` once the worker confirms ``applied_route_version > 0``.
    ``pick_worker`` candidate set is gated on ``status == ALIVE``, so the
    new value silently prevents traffic assignment during cold-start.

Schema impact:
    ``workers.status`` is persisted as ``VARCHAR(64)`` via
    ``StrEnumType(WorkerStatus)`` (see
    ``src/ai/backend/appproxy/coordinator/models/base.py``). There is no
    Postgres ENUM type and no CHECK constraint to alter — the enum is
    enforced purely in Python by ``StrEnumType.process_result_value``.
    Adding a new value is therefore a zero-DDL change at the database
    level; this migration exists to give operators an explicit revision
    marker in the alembic history and to leave room for backports onto
    branches that DO model the column as a Postgres enum (Backend.AI
    deployments which forked the schema for tighter constraints).

    If a downstream branch promoted ``workers.status`` to a real
    ``CREATE TYPE workerstatus AS ENUM (...)``, the upgrade() body below
    will detect it and run ``ALTER TYPE ... ADD VALUE IF NOT EXISTS``.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "d2b5a8e9c1f4"
down_revision = "c1a4f7b2e8d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _enum_type_exists(conn: sa.engine.Connection, type_name: str) -> bool:
    """Return True iff a Postgres ENUM type with ``type_name`` exists.

    Used to make this migration safe across branches that model
    ``workers.status`` as either ``VARCHAR(64)`` (main) or
    ``CREATE TYPE workerstatus AS ENUM (...)`` (some downstream forks).
    """
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM pg_type t "
            "JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace "
            "WHERE t.typname = :n AND t.typtype = 'e'"
        ),
        {"n": type_name},
    )
    return result.first() is not None


def _check_constraint_exists(conn: sa.engine.Connection, name: str) -> bool:
    """Return True iff a named CHECK constraint exists on this DB.

    Some forks enforce the ``workers.status`` allow-list via a CHECK
    constraint instead of a Postgres ENUM. We detect the constraint by
    name and let the operator drop/recreate it manually — automating that
    here would require parsing the constraint expression and isn't worth
    the risk for a guard that only fires on non-main forks.
    """
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_constraint WHERE conname = :n"),
        {"n": name},
    )
    return result.first() is not None


def upgrade() -> None:
    conn = op.get_bind()

    # Path A — main branch: status is plain VARCHAR(64) enforced by
    # ``StrEnumType`` in Python. Nothing to do at the SQL level; the new
    # ``STARTING`` value becomes valid as soon as the application code
    # ships. This branch is the no-op path in production.
    #
    # Path B — fork with a real Postgres enum named ``workerstatus``.
    # ``ALTER TYPE ... ADD VALUE`` cannot run inside a transaction block
    # in older Postgres versions, so we use the ``IF NOT EXISTS`` form
    # (Postgres >= 9.6) and execute it on its own statement. Operators on
    # ancient Postgres should run this migration with
    # ``--sql`` / autocommit mode.
    if _enum_type_exists(conn, "workerstatus"):
        op.execute("ALTER TYPE workerstatus ADD VALUE IF NOT EXISTS 'STARTING'")

    # Path C — fork using a CHECK constraint to enforce the allow-list.
    # We surface the situation in the alembic log instead of silently
    # passing; the operator must drop & recreate the constraint to admit
    # ``STARTING``. The migration intentionally does NOT fail so the
    # migration history stays linear across forks.
    if _check_constraint_exists(conn, "ck_workers_status"):
        # NOTE: deliberately not raising — leave a breadcrumb for ops.
        # The application keeps writing 'ALIVE' on the legacy path until
        # the constraint is widened to admit 'STARTING'.
        op.execute(
            "COMMENT ON CONSTRAINT ck_workers_status ON workers IS "
            "'OPS-2: widen this CHECK to include ''STARTING'' "
            "(see migration d2b5a8e9c1f4)'"
        )


def downgrade() -> None:
    # Removing an enum value from a Postgres ENUM type is non-trivial:
    # Postgres has no ``ALTER TYPE ... DROP VALUE`` statement. Doing it
    # manually requires
    #   1. Renaming the existing type out of the way.
    #   2. Recreating the type without the offending value.
    #   3. Rewriting every column reference to use the new type.
    #   4. Backfilling any rows still holding the removed value.
    #
    # Since ``workers.status`` is VARCHAR on main (no enum to alter), the
    # downgrade path is a true no-op in production. On forks that DO use
    # an enum, the operator must perform the rename/recreate dance
    # manually before downgrading further; doing it automatically here
    # would risk corrupting live worker rows that are mid-cold-start.
    pass
