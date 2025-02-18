"""add scixid to records db

Revision ID: 82b35eed1cd4
Revises: 2d2af8a9c996
Create Date: 2024-10-15 22:07:29.923858

"""

# revision identifiers, used by Alembic.
revision = '82b35eed1cd4'
down_revision = '2d2af8a9c996'

from alembic import op
from sqlalchemy import Column, String, Integer, TIMESTAMP, DateTime, Text, Index, Boolean

                               


def upgrade():
    op.add_column('records', Column('scix_id', String(19), nullable = True, default=None, unique=True))
    

def downgrade():
    op.drop_column('records', 'scix_id')
