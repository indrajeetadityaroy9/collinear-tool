from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional

# Define the impact level as an enum for better type safety
class ImpactLevel(str, Enum):
    NA = "not_available"  # New category for when size information is unavailable
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

# Define metrics model for impact assessment
class DatasetMetrics(BaseModel):
    size_bytes: Optional[int] = Field(None, description="Size of the dataset in bytes")
    file_count: Optional[int] = Field(None, description="Number of files in the dataset")
    downloads: Optional[int] = Field(None, description="Number of downloads (all time)")
    likes: Optional[int] = Field(None, description="Number of likes") 