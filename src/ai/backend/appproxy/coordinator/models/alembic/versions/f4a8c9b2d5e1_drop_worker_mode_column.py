"""Drop workers.mode column.

The ``mode`` column (``self-hosted`` / ``external-backend``) is fully
determined by ``backend_kind`` — NATIVE → ``self-hosted``, TRAEFIK and
CONTINUUM → ``external-backend``. Keeping it as a stored column duplicated
state and risked drift; callers now derive the value via
``BackendKind.worker_mode`` at the type level.

The downgrade path reintroduces the column and backfills it from
``backend_kind`` so an older Coordinator image can run against the
restored schema without rebuilding the row set.

Revision ID: f4a8c9b2d5e1
Revises: e3c6b9a2d7f5
Create Date: 2024-05-01 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f4a8c9b2d5e1"
down_revision = "e3c6b9a2d7f5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("workers", "mode")


def downgrade() -> None:
    op.add_column(
        "workers",
        sa.Column(
            "mode",
            sa.String(length=32),
            nullable=False,
            server_default="self-hosted",
        ),
    )
    op.execute(
        sa.text(
            "UPDATE workers SET mode = "
            "CASE WHEN backend_kind = 'native' THEN 'self-hosted' "
            "ELSE 'external-backend' END"
        )
    )
