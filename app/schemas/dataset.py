from enum import Enum
from pydantic import BaseModel, Field
from typing import Dict, Literal, Optional, List

# Define the impact level as an enum for better type safety
class ImpactLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

# Define metrics for impact assessment
class DatasetMetrics(BaseModel):
    size_bytes: Optional[int] = Field(None, description="Size of the dataset in bytes")
    file_count: Optional[int] = Field(None, description="Number of files in the dataset")
    downloads: Optional[int] = Field(None, description="Number of downloads (all time)")
    likes: Optional[int] = Field(None, description="Number of likes")

# Define a schema for impact assessment response
class ImpactAssessment(BaseModel):
    dataset_id: str = Field(..., description="The ID of the dataset being assessed")
    impact_level: ImpactLevel = Field(..., description="The impact level: low, medium, or high")
    assessment_method: str = Field(
        "unknown", 
        description="Method used to determine impact level (e.g., size_based, downloads_and_likes_based)"
    )
    metrics: DatasetMetrics = Field(
        ...,
        description="Metrics used for impact assessment"
    )
    thresholds: Dict[str, Dict[str, str]] = Field(
        ...,
        description="The thresholds used for determining impact levels for each metric",
    )

# Define a schema for dataset list response
class DatasetInfo(BaseModel):
    id: str
    impact_level: Optional[ImpactLevel] = None
    impact_assessment: Optional[Dict] = None
    # Add other fields as needed
    
    class Config:
        extra = "allow"  # Allow extra fields from the API

# Define base dataset schema for shared attributes
class DatasetBase(BaseModel):
    name: str
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    
# Define schema for dataset creation
class DatasetCreate(DatasetBase):
    # Add fields specific to creation
    # For example, file paths or URLs
    files: Optional[List[str]] = None

# Define schema for dataset update
class DatasetUpdate(DatasetBase):
    name: Optional[str] = None  # Make fields optional for updates
    
# Define complete dataset schema with all fields
class Dataset(DatasetBase):
    id: int  # or str depending on your ID format
    owner_id: str  # Assuming user IDs are strings
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    
    class Config:
        orm_mode = True  # For ORM compatibility if using an ORM

__all__ = ["ImpactLevel", "ImpactAssessment", "DatasetInfo", "DatasetMetrics", 
           "Dataset", "DatasetCreate", "DatasetUpdate"] 