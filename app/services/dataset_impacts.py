"""Dataset impact assessment storage and retrieval."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

from app.schemas.dataset import ImpactLevel
from app.supabase import require_supabase
from app.core.config import settings

log = logging.getLogger(__name__)

async def save_dataset_impact(dataset_id: str, impact_data: Dict[str, Any], jwt: str = None) -> None:
    """
    Save dataset impact assessment to the database.
    
    Args:
        dataset_id: The unique identifier of the dataset
        impact_data: Impact assessment data with metrics and impact level
        jwt: Optional JWT for authenticated access
    """
    try:
        # Use service key for authenticated access
        db = require_supabase(jwt)
        
        # Convert enum to string if needed
        impact_level = impact_data["impact_level"]
        if hasattr(impact_level, "value"):
            impact_level = impact_level.value
        
        # Replace None/NULL values with 0 for numeric metrics
        metrics = impact_data["metrics"].copy()
        for key in ["size_bytes", "file_count", "downloads", "likes"]:
            if metrics.get(key) is None:
                metrics[key] = 0
                log.info(f"Replaced NULL value with 0 for {key} in dataset {dataset_id}")
        
        # Prepare data for insertion
        impact_record = {
            "dataset_id": dataset_id,
            "impact_level": impact_level,
            "size_bytes": metrics["size_bytes"],
            "file_count": metrics["file_count"],
            "downloads": metrics["downloads"],
            "likes": metrics["likes"],
            "assessment_method": impact_data["assessment_method"],
            "last_assessed_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Insert or update the impact assessment
        log.info(f"Saving impact assessment for dataset {dataset_id}")
        result = await asyncio.to_thread(
            lambda: db.table("dataset_impacts")
            .upsert(impact_record)
            .execute()
        )
        log.info(f"Successfully saved impact for {dataset_id}: {impact_level}")
        return result
    except Exception as e:
        log.error(f"Error saving impact for {dataset_id}: {e}")
        # Don't re-raise the exception to avoid breaking the API response
        # Just log it and continue

async def get_stored_dataset_impact(dataset_id: str, jwt: str = None) -> Optional[Dict[str, Any]]:
    """
    Retrieve a stored dataset impact assessment.
    
    Args:
        dataset_id: The unique identifier of the dataset
        jwt: Optional JWT for authenticated access
        
    Returns:
        Dictionary with impact assessment data if found, None otherwise
    """
    db = require_supabase(jwt)
    
    result = await asyncio.to_thread(
        lambda: db.table("dataset_impacts")
        .select("*")
        .eq("dataset_id", dataset_id)
        .execute()
    )
    
    if result.data and len(result.data) > 0:
        return result.data[0]
    return None

async def is_impact_assessment_stale(stored_impact: Dict[str, Any], max_age_days: int = 7) -> bool:
    """
    Check if a stored impact assessment is stale (older than max_age_days).
    
    Args:
        stored_impact: Stored impact assessment data
        max_age_days: Maximum age in days before considered stale
        
    Returns:
        True if assessment is stale, False otherwise
    """
    if not stored_impact or "last_assessed_at" not in stored_impact:
        return True
        
    try:
        last_assessed = datetime.fromisoformat(stored_impact["last_assessed_at"])
        max_age = timedelta(days=max_age_days)
        return (datetime.now(timezone.utc) - last_assessed) > max_age
    except (ValueError, TypeError):
        # If date parsing fails, consider it stale
        return True

async def list_datasets_by_impact(impact_level: ImpactLevel, limit: int = 100, jwt: str = None) -> list[str]:
    """
    List dataset IDs with a specific impact level.
    
    Args:
        impact_level: The impact level to filter by
        limit: Maximum number of results to return
        jwt: Optional JWT for authenticated access
        
    Returns:
        List of dataset IDs with the specified impact level
    """
    db = require_supabase(jwt)
    
    result = await asyncio.to_thread(
        lambda: db.table("dataset_impacts")
        .select("dataset_id")
        .eq("impact_level", impact_level.value)
        .order("last_assessed_at", desc=True)
        .limit(limit)
        .execute()
    )
    
    if result.data:
        return [item["dataset_id"] for item in result.data]
    return []

async def populate_impact_assessments(dataset_ids: List[str], jwt: str = None) -> Dict[str, str]:
    """
    Populate impact assessments for a batch of datasets.
    
    This function is useful for background jobs that pre-calculate impact assessments
    for datasets to avoid doing it on-demand.
    
    Args:
        dataset_ids: List of dataset IDs to assess
        jwt: Optional JWT for authenticated access
        
    Returns:
        Dictionary mapping dataset_ids to their impact levels
    """
    from app.services.hf_datasets import get_dataset_impact_async
    
    results = {}
    errors = []
    
    # Process datasets in parallel with a limit on concurrency
    semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests
    
    async def process_dataset(dataset_id: str):
        async with semaphore:
            try:
                log.info(f"Calculating impact for dataset: {dataset_id}")
                impact_data = await get_dataset_impact_async(dataset_id)
                await save_dataset_impact(dataset_id, impact_data, jwt)
                results[dataset_id] = impact_data["impact_level"].value
                log.info(f"Saved impact for {dataset_id}: {impact_data['impact_level'].value}")
            except Exception as e:
                log.error(f"Error calculating impact for {dataset_id}: {e}")
                errors.append((dataset_id, str(e)))
    
    # Create tasks for all datasets
    tasks = [process_dataset(dataset_id) for dataset_id in dataset_ids]
    
    # Run all tasks
    await asyncio.gather(*tasks)
    
    if errors:
        log.warning(f"Encountered {len(errors)} errors during impact assessment population")
    
    return results

# Export the functions
__all__ = [
    "save_dataset_impact",
    "get_stored_dataset_impact",
    "is_impact_assessment_stale",
    "list_datasets_by_impact",
    "populate_impact_assessments"
] 