"""add_allowed_ip_column

Revision ID: f83d630c0bc9
Revises: 35923972eddb
Create Date: 2022-08-31 16:43:43.188564

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f83d630c0bc9"
down_revision = "35923972eddb"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "users", sa.Column("allowed_client_ip", postgresql.ARRAY(postgresql.CIDR()), nullable=True)
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("users", "allowed_client_ip")
    # ### end Alembic commands ###