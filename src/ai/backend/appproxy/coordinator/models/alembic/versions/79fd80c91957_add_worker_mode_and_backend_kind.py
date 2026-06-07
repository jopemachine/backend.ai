"""Add worker.mode and worker.backend_kind columns.

Adds two new columns to the ``workers`` table to make the worker's
operation mode and data-plane backend kind first-class. Existing rows are
backfilled with a heuristic based on ``traefik_last_used_marker_path``.

This migration is idempotent so that backporting to release branches is safe
(see ``src/ai/backend/manager/models/alembic/README.md`` for the policy).

Revision ID: 79fd80c91957
Revises: a1b2c3d4e5f6
Create Date: 2026-06-04 19:30:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "79fd80c91957"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


SELF_HOSTED = "self-hosted"
EXTERNAL_BACKEND = "external-backend"
NATIVE = "native"
TRAEFIK = "traefik"


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(c["name"] == column for c in inspector.get_columns(table))


def upgrade() -> None:
    # Idempotency: skip if both columns already exist (re-run safety / backport).
    add_mode = not _has_column("workers", "mode")
    add_backend_kind = not _has_column("workers", "backend_kind")

    if add_mode:
        op.add_column(
            "workers",
            sa.Column(
                "mode",
                sa.VARCHAR(64),
                server_default=SELF_HOSTED,
                nullable=False,
            ),
        )
    if add_backend_kind:
        op.add_column(
            "workers",
            sa.Column(
                "backend_kind",
                sa.VARCHAR(64),
                server_default=NATIVE,
                nullable=False,
            ),
        )

    # Backfill: rows whose traefik_last_used_marker_path is set are Traefik-backend
    # workers; all others remain self-hosted+native (matches the server_default).
    if add_mode or add_backend_kind:
        op.execute(
            "UPDATE workers "
            f"SET mode = '{EXTERNAL_BACKEND}', backend_kind = '{TRAEFIK}' "
            "WHERE traefik_last_used_marker_path IS NOT NULL"
        )


def downgrade() -> None:
    if _has_column("workers", "backend_kind"):
        op.drop_column("workers", "backend_kind")
    if _has_column("workers", "mode"):
        op.drop_column("workers", "mode")
