# app/db/base.py
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import registry
from sqlalchemy import MetaData, Table
from app.settings import settings

engine = create_async_engine(settings.database_url, echo=False, future=True)
mapper_registry = registry()
metadata = MetaData()

dataset_follows = Table(
    "dataset_follows",
    metadata,
    autoload_with=engine.sync_engine,
)

class DatasetFollow:
    pass

mapper_registry.map_imperatively(DatasetFollow, dataset_follows)
