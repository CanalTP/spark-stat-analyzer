"""coverage_journeys_types

Revision ID: 455c95b7c1b2
Revises: 1529d296e0ae
Create Date: 2017-03-17 15:41:10.377122

"""

# revision identifiers, used by Alembic.
revision = '455c95b7c1b2'
down_revision = '1529d296e0ae'

from alembic import op
import sqlalchemy as sa
from migrations.utils import get_create_partition_sql_func, get_drop_partition_sql_func, \
    get_create_trigger_sql, get_drop_table_cascade_sql
import config


table = "coverage_journeys_types"
schema = config.db['schema']

def upgrade():
    op.create_table(table, sa.Column('request_date', sa.DateTime(), nullable=False),
                    sa.Column('region_id', sa.Text(), nullable=False),
                    sa.Column('from_type', sa.Text(), nullable=False),
                    sa.Column('to_type', sa.Text(), nullable=False),
                    sa.Column('nb', sa.BigInteger(), nullable=False),
                    sa.PrimaryKeyConstraint('request_date', 'region_id', 'from_type', 'to_type'),
                    schema=schema)

    op.execute(get_create_partition_sql_func(schema, table))
    op.execute(get_create_trigger_sql(schema, table))


def downgrade():
    op.execute(get_drop_table_cascade_sql(schema, table))
    op.execute(get_drop_partition_sql_func(table))