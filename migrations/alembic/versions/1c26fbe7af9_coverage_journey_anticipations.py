"""coverage_journey_anticipations

Revision ID: 1c26fbe7af9
Revises: 1cc79244cdf
Create Date: 2017-03-02 16:21:47.251220

"""

# revision identifiers, used by Alembic.
revision = '1c26fbe7af9'
down_revision = '1cc79244cdf'

from alembic import op
import sqlalchemy as sa
import config

table = "coverage_journey_anticipations"


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
                                            schema=config.db['schema'])),
                    schema=config.db['schema']
                    )

    op.execute("""
        CREATE OR REPLACE FUNCTION {table}_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := '{schema}';
              partition := '{table}' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE 'A partition has been created %',partition;
                EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition ||
                        ' (
                          check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || '''
                                  AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' ||
                        'INHERITS (' || schema || '.{table});';
              END IF;
              EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.{table}' || ' ' || quote_literal(NEW) || ').*;';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        """.format(
        schema=config.db['schema'], table=table
    ))
    op.execute("""
        CREATE TRIGGER insert_{table}_trigger
            BEFORE INSERT ON {schema}.{table}
            FOR EACH ROW EXECUTE PROCEDURE {table}_insert_trigger();
        """.format(
        table=table,
        schema=config.db['schema']
    ))


def downgrade():
    op.drop_table('coverage_journey_anticipations', schema=config.db['schema'])
    op.execute("""DROP FUNCTION IF EXISTS {table}_insert_trigger();""".format(
        table=table
    ))
