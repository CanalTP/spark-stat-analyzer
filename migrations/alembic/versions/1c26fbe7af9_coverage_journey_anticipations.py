"""coverage_journey_anticipations

Revision ID: 1c26fbe7af9
Revises: 1cc79244cdf
Create Date: 2017-03-02 16:21:47.251220

"""

# revision identifiers, used by Alembic.
revision = '1c26fbe7af9'
down_revision = '48218f5ebd1c'

from alembic import op
import sqlalchemy as sa
import config
from migrations.utils import get_create_partition_sql_func, get_drop_partition_sql_func, \
                             get_create_trigger_sql

table = "coverage_journey_anticipations"
schema = config.db['schema']


def upgrade():
    op.create_table('coverage_journey_anticipations',
                    sa.Column('region_id', sa.Text(), nullable=False),
                    sa.Column('is_internal_call', sa.SmallInteger(), nullable=False),
                    sa.Column('request_date', sa.DateTime(), nullable=True),
                    sa.Column('difference', sa.Integer(), nullable=True),
                    sa.Column('nb', sa.BigInteger(), nullable=True),
                    sa.PrimaryKeyConstraint('region_id', 'is_internal_call'),
                    sa.UniqueConstraint('region_id', 'is_internal_call', 'request_date',
                                        name='{schema}_coverage_journey_anticipations_pkey'.format(
                                            schema=schema)),
                    schema=schema
                    )

    op.execute(get_create_partition_sql_func(schema, table))
    op.execute(get_create_trigger_sql(schema, table))


def downgrade():
    op.drop_table(table, schema=schema)
    op.execute(get_drop_partition_sql_func(table))
