"""workers.authority unique → (authority, backend_kind) composite

Revision ID: b8e5c4a17f02
Revises: 79fd80c91957
Create Date: 2026-06-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "b8e5c4a17f02"
down_revision = "79fd80c91957"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _index_exists(conn: sa.engine.Connection, name: str) -> bool:
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :n"),
        {"n": name},
    )
    return result.first() is not None


def _constraint_exists(conn: sa.engine.Connection, name: str) -> bool:
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_constraint WHERE conname = :n"),
        {"n": name},
    )
    return result.first() is not None


def upgrade() -> None:
    conn = op.get_bind()

    # Drop legacy single-column uniqueness (auto-generated name varies across
    # historical heads — try both common forms before bailing out).
    for legacy_name in ("workers_authority_key", "uq_workers_authority"):
        if _constraint_exists(conn, legacy_name):
            op.drop_constraint(legacy_name, "workers", type_="unique")
            break
    if _index_exists(conn, "workers_authority_key"):
        op.drop_index("workers_authority_key", table_name="workers")

    # Add composite uniqueness; idempotent for re-runs and backport branches.
    if not _constraint_exists(conn, "uq_workers_authority_backend_kind"):
        op.create_unique_constraint(
            "uq_workers_authority_backend_kind",
            "workers",
            ["authority", "backend_kind"],
        )


def downgrade() -> None:
    conn = op.get_bind()
    if _constraint_exists(conn, "uq_workers_authority_backend_kind"):
        op.drop_constraint("uq_workers_authority_backend_kind", "workers", type_="unique")
    if not _constraint_exists(conn, "workers_authority_key"):
        op.create_unique_constraint("workers_authority_key", "workers", ["authority"])
