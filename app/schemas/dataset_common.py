from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional


class ImpactLevel(str, Enum):
    NA = "not_available"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class DatasetMetrics(BaseModel):
    size_bytes: Optional[int] = Field(None, description="Size of the dataset in bytes")
    file_count: Optional[int] = Field(None, description="Number of files in the dataset")
    downloads: Optional[int] = Field(None, description="Number of downloads (all time)")
    likes: Optional[int] = Field(None, description="Number of likes")