"""Add nb_with_odt column from coverage_journeys

Revision ID: 2dc7abaa8047
Revises: b4902311ea96
Create Date: 2017-03-14 13:02:53.031755

"""

# revision identifiers, used by Alembic.
revision = '2dc7abaa8047'
down_revision = 'b4902311ea96'

from alembic import op
import sqlalchemy as sa
import config

schema = config.db['schema']

def upgrade():
    op.add_column('coverage_journeys', sa.Column('nb_with_odt', sa.BigInteger(), nullable=True), schema=schema)

def downgrade():
    op.drop_column('coverage_journeys', 'nb_with_odt', schema=schema)
