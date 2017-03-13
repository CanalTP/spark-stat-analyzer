"""Remove nb_without_journeys column from error_stats

Revision ID: 51007d933cba
Revises: 22e888ef666
Create Date: 2017-03-13 13:34:57.590632

"""

# revision identifiers, used by Alembic.
revision = '51007d933cba'
down_revision = '22e888ef666'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.drop_column('error_stats', 'nb_without_journey')


def downgrade():
    op.add_column('error_stats', sa.Column('nb_without_journey', sa.BigInteger(), nullable=True))
