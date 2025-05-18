# app/models/dataset_follows.py
from datetime import datetime
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base

class DatasetFollow(Base):
    __tablename__  = "dataset_follows"
    __table_args__ = (
        UniqueConstraint("user_id", "dataset_id", name="uq_user_dataset"),
    )

    id:          Mapped[int]      = mapped_column(primary_key=True, autoincrement=True)
    user_id:     Mapped[str]      = mapped_column(UUID, ForeignKey("auth.users.id", ondelete="CASCADE"))
    dataset_id:  Mapped[str]      = mapped_column(nullable=False, index=True)
    followed_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
