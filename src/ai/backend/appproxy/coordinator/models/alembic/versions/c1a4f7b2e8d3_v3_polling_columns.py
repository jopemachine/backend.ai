"""workers v3 polling columns (last_polled_at only)

Revision ID: c1a4f7b2e8d3
Revises: 79fd80c91957
Create Date: 2026-06-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "c1a4f7b2e8d3"
down_revision = "79fd80c91957"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _column_exists(conn: sa.engine.Connection, table: str, name: str) -> bool:
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns WHERE table_name = :t AND column_name = :n"
        ),
        {"t": table, "n": name},
    )
    return result.first() is not None


def upgrade() -> None:
    conn = op.get_bind()

    if not _column_exists(conn, "workers", "last_polled_at"):
        op.add_column(
            "workers",
            sa.Column(
                "last_polled_at",
                sa.TIMESTAMP(timezone=True),
                nullable=True,
            ),
        )


def downgrade() -> None:
    conn = op.get_bind()

    if _column_exists(conn, "workers", "last_polled_at"):
        op.drop_column("workers", "last_polled_at")
