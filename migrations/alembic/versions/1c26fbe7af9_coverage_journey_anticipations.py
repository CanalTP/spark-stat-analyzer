"""coverage_journey_anticipations

Revision ID: 1c26fbe7af9
Revises: 48218f5ebd1c
Create Date: 2017-03-02 16:21:47.251220

"""

# revision identifiers, used by Alembic.
revision = '1c26fbe7af9'
down_revision = '48218f5ebd1c'

from alembic import op
import sqlalchemy as sa
import config
from migrations.utils import get_create_partition_sql_func, get_drop_partition_sql_func, \
    get_create_trigger_sql, get_drop_table_cascade_sql

table = "coverage_journey_anticipations"
schema = config.db['schema']


def upgrade():
    op.create_table(table,
                    sa.Column('region_id', sa.Text(), nullable=False),
                    sa.Column('is_internal_call', sa.SmallInteger(), nullable=False),
                    sa.Column('request_date', sa.DateTime(), nullable=False),
                    sa.Column('difference', sa.Integer(), nullable=False),
                    sa.Column('nb', sa.BigInteger(), nullable=True),
                    sa.PrimaryKeyConstraint('region_id', 'is_internal_call', 'request_date', 'difference'),
                    schema=schema
                    )

    op.execute(get_create_partition_sql_func(schema, table))
    op.execute(get_create_trigger_sql(schema, table))


def downgrade():
    op.execute(get_drop_table_cascade_sql(schema, table))
    op.execute(get_drop_partition_sql_func(table))
