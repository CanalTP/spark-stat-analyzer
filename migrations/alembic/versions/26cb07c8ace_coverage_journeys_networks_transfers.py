"""coverage_journeys_networks_transfers

Revision ID: 26cb07c8ace
Revises: 2dc7abaa8047
Create Date: 2017-03-16 11:28:54.743892

"""

# revision identifiers, used by Alembic.
revision = '26cb07c8ace'
down_revision = '2dc7abaa8047'

from alembic import op
import sqlalchemy as sa
import config
from migrations.utils import get_create_partition_sql_func, get_drop_partition_sql_func, \
    get_create_trigger_sql, get_drop_table_cascade_sql

table = "coverage_journeys_networks_transfers"
schema = config.db['schema']


def upgrade():
    op.create_table('coverage_journeys_networks_transfers',
                    sa.Column('request_date', sa.DateTime(), nullable=False),
                    sa.Column('region_id', sa.Text(), nullable=False),
                    sa.Column('is_internal_call', sa.SmallInteger(), nullable=False),
                    sa.Column('nb_transfers', sa.Integer(), nullable=False),
                    sa.Column('nb_networks', sa.Integer(), nullable=False),
                    sa.Column('nb_journeys', sa.BigInteger(), nullable=False),
                    sa.PrimaryKeyConstraint('request_date', 'region_id', 'is_internal_call', 'nb_transfers',
                                            'nb_networks'),
                    schema=schema
                    )

    op.execute(get_create_partition_sql_func(schema, table))
    op.execute(get_create_trigger_sql(schema, table))


def downgrade():
    op.execute(get_drop_table_cascade_sql(schema, table))
    op.execute(get_drop_partition_sql_func(table))
