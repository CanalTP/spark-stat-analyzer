"""triggers from stat-compiler

Revision ID: 22e888ef666
Revises: 1c26fbe7af9
Create Date: 2017-03-02 15:20:51.315068

"""

# revision identifiers, used by Alembic.
revision = '22e888ef666'
down_revision = '1c26fbe7af9'

from alembic import op
import config
from migrations.utils import get_create_partition_sql_func, get_create_trigger_sql

schema = config.db['schema']
tables_need_trigger = ['coverage_journeys', 'coverage_journeys_requests_params', 'coverage_journeys_transfers',
                       'coverage_modes', 'coverage_networks', 'coverage_stop_areas', 'error_stats', 'requests_calls',
                       'token_stats']


def upgrade():
    context = op.get_context()
    connection = op.get_bind()
    for table in tables_need_trigger:
        if not context.dialect.has_table(connection.engine, table_name=table, schema=schema):
            op.execute(get_create_partition_sql_func(schema, table))
            op.execute(get_create_trigger_sql(schema, table))


def downgrade():
    pass
