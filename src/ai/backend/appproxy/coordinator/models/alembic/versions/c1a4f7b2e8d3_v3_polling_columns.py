"""workers v3 polling columns + worker_route_version_seq

Revision ID: c1a4f7b2e8d3
Revises: b8e5c4a17f02
Create Date: 2026-06-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c1a4f7b2e8d3"
down_revision = "b8e5c4a17f02"
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


def _constraint_exists(conn: sa.engine.Connection, name: str) -> bool:
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_constraint WHERE conname = :n"),
        {"n": name},
    )
    return result.first() is not None


def _index_exists(conn: sa.engine.Connection, name: str) -> bool:
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :n"),
        {"n": name},
    )
    return result.first() is not None


def _sequence_exists(conn: sa.engine.Connection, name: str) -> bool:
    result = conn.execute(
        sa.text("SELECT 1 FROM information_schema.sequences WHERE sequence_name = :n"),
        {"n": name},
    )
    return result.first() is not None


def upgrade() -> None:
    conn = op.get_bind()

    if not _column_exists(conn, "workers", "capabilities"):
        op.add_column(
            "workers",
            sa.Column(
                "capabilities",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
        )

    if not _column_exists(conn, "workers", "scope"):
        op.add_column(
            "workers",
            sa.Column(
                "scope",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
        )

    if not _column_exists(conn, "workers", "poll_token_revision"):
        op.add_column(
            "workers",
            sa.Column(
                "poll_token_revision",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )

    if not _column_exists(conn, "workers", "committed_route_version"):
        op.add_column(
            "workers",
            sa.Column(
                "committed_route_version",
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )

    if not _column_exists(conn, "workers", "last_polled_at"):
        op.add_column(
            "workers",
            sa.Column(
                "last_polled_at",
                sa.TIMESTAMP(timezone=True),
                nullable=True,
            ),
        )

    # Per-worker route version sequence; safe across re-runs and backports.
    op.execute("CREATE SEQUENCE IF NOT EXISTS worker_route_version_seq AS BIGINT START 1")


def downgrade() -> None:
    conn = op.get_bind()

    op.execute("DROP SEQUENCE IF EXISTS worker_route_version_seq")

    for column_name in (
        "last_polled_at",
        "committed_route_version",
        "poll_token_revision",
        "scope",
        "capabilities",
    ):
        if _column_exists(conn, "workers", column_name):
            op.drop_column("workers", column_name)
