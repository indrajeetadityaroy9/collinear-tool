import uuid
import hashlib
import logging
from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field

from app.schemas.dataset_common import ImpactLevel, DatasetMetrics

# Log for this module
log = logging.getLogger(__name__)

# Supported strategies for dataset combination
SUPPORTED_STRATEGIES = ["merge", "intersect", "filter"]

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
        {},
        description="Thresholds used for determining impact levels (for reference)"
    )

class DatasetInfo(BaseModel):
    id: str
    impact_level: Optional[ImpactLevel] = None
    impact_assessment: Optional[Dict] = None
    # Add other fields as needed
    class Config:
        extra = "allow"  # Allow extra fields from the API

class DatasetBase(BaseModel):
    name: str
    description: Optional[str] = None
    tags: Optional[List[str]] = None

class DatasetCreate(DatasetBase):
    files: Optional[List[str]] = None

class DatasetUpdate(DatasetBase):
    name: Optional[str] = None  # Make fields optional for updates

class Dataset(DatasetBase):
    id: int  # or str depending on your ID format
    owner_id: str  # Assuming user IDs are strings
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    class Config:
        pass  # Removed orm_mode = True since ORM is not used

class DatasetCombineRequest(BaseModel):
    source_datasets: List[str] = Field(..., description="List of dataset IDs to combine")
    name: str = Field(..., description="Name for the combined dataset")
    description: Optional[str] = Field(None, description="Description for the combined dataset")
    combination_strategy: str = Field("merge", description="Strategy to use when combining datasets (e.g., 'merge', 'intersect', 'filter')")
    filter_criteria: Optional[Dict[str, Any]] = Field(None, description="Criteria for filtering when combining datasets")

class CombinedDataset(BaseModel):
    id: str = Field(..., description="ID of the combined dataset")
    name: str = Field(..., description="Name of the combined dataset")
    description: Optional[str] = Field(None, description="Description of the combined dataset")
    source_datasets: List[str] = Field(..., description="IDs of the source datasets")
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: str = Field(..., description="ID of the user who created this combined dataset")
    impact_level: Optional[ImpactLevel] = Field(None, description="Calculated impact level of the combined dataset")
    status: str = Field("processing", description="Status of the dataset combination process")
    combination_strategy: str = Field(..., description="Strategy used when combining datasets")
    metrics: Optional[DatasetMetrics] = Field(None, description="Metrics for the combined dataset")
    storage_bucket_id: Optional[str] = Field(None, description="ID of the storage bucket containing dataset files")
    storage_folder_path: Optional[str] = Field(None, description="Path to the dataset files within the bucket")
    class Config:
        extra = "allow"  # Allow extra fields for flexibility

__all__ = ["ImpactLevel", "ImpactAssessment", "DatasetInfo", "DatasetMetrics", 
           "Dataset", "DatasetCreate", "DatasetUpdate", "DatasetCombineRequest", "CombinedDataset"] 