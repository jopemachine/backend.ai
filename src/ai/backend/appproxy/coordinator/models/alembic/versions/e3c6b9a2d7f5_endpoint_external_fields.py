"""endpoints external backend kind / credential ref / model aliases (OB-COMP-2)

Revision ID: e3c6b9a2d7f5
Revises: d2b5a8e9c1f4
Create Date: 2026-06-06

OB-COMP-2 mitigation — the manager-side bulk endpoint push DTO grew three
additive fields that describe routes whose upstream is an external API
rather than a Backend.AI kernel session. The coordinator now persists those
fields onto the ``endpoints`` row so the v3 route-snapshot service can fan
them out to workers via ``ReplicaDTO.backend_type`` / ``credential_ref`` /
``models`` without re-querying the manager on every poll.

All three columns are introduced with legacy-equivalent defaults so the
migration is backward-compatible:

* ``external_backend_type`` (VARCHAR(64), NOT NULL, default ``'ba_kernel'``)
* ``credential_ref`` (VARCHAR(255), NULL)
* ``model_aliases`` (JSONB, NOT NULL, default ``'[]'::jsonb``)

The upgrade is idempotent — each ``ALTER TABLE`` is guarded by an
``information_schema`` lookup so the same migration can run against a fresh
DB or an environment that already received the columns via a hot-fix.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e3c6b9a2d7f5"
down_revision = "d2b5a8e9c1f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _column_exists(conn: sa.engine.Connection, table: str, name: str) -> bool:
    """Return True iff the column already exists on the target table.

    Used to make the migration idempotent so it can be re-run safely against
    databases that already received the columns via an out-of-band hot-fix.
    """
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns WHERE table_name = :t AND column_name = :n"
        ),
        {"t": table, "n": name},
    )
    return result.first() is not None


def upgrade() -> None:
    conn = op.get_bind()

    if not _column_exists(conn, "endpoints", "external_backend_type"):
        op.add_column(
            "endpoints",
            sa.Column(
                "external_backend_type",
                sa.String(length=64),
                nullable=False,
                server_default=sa.text("'ba_kernel'"),
            ),
        )

    if not _column_exists(conn, "endpoints", "credential_ref"):
        op.add_column(
            "endpoints",
            sa.Column(
                "credential_ref",
                sa.String(length=255),
                nullable=True,
            ),
        )

    if not _column_exists(conn, "endpoints", "model_aliases"):
        op.add_column(
            "endpoints",
            sa.Column(
                "model_aliases",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            ),
        )


def downgrade() -> None:
    conn = op.get_bind()

    for column_name in ("model_aliases", "credential_ref", "external_backend_type"):
        if _column_exists(conn, "endpoints", column_name):
            op.drop_column("endpoints", column_name)
