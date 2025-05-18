from sqlalchemy import MetaData, Table, Column
from sqlalchemy.dialects.postgresql import UUID
from app.db import Base, engine

class AuthUser(Base):
    __tablename__   = "users"
    __table_args__  = {"schema": "auth"}
    id = Column(UUID, primary_key=True)

    __mapper_args__ = {"primary_key": [id]}
