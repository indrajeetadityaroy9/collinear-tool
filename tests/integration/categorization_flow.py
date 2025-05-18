"""
1. Fetching datasets from the HuggingFace API
2. Saving impact assessments to the database
3. Retrieving stored assessments
4. Checking if assessments are stale
5. Listing datasets by impact level
6. Batch population of assessments
"""

import asyncio
import json
import logging
import os
import sys
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("impact-test")


load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api")

class ImpactTestClient:
    """Client to test dataset impact assessment functionality."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.headers = {"Content-Type": "application/json"}
    
    async def get_datasets(self, limit: int = 10, with_impact: bool = False) -> List[Dict[str, Any]]:
        """Fetch datasets from the API."""
        url = f"{self.base_url}/datasets?limit={limit}&with_impact={str(with_impact).lower()}"
        log.info(f"Fetching datasets from: {url}")
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        datasets = response.json()
        
        log.info(f"Fetched {len(datasets)} datasets")
        return datasets
    
    async def get_dataset_impact(self, dataset_id: str, force_refresh: bool = False) -> Dict[str, Any]:
        """Get impact assessment for a specific dataset."""
        url = f"{self.base_url}/datasets/{dataset_id}/impact"
        if force_refresh:
            url += "?force_refresh=true"
            
        log.info(f"Getting impact for dataset: {dataset_id}")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        impact = response.json()
        
        log.info(f"Dataset {dataset_id} has impact level: {impact.get('impact_level')}")
        return impact
    
    async def populate_impacts(self, dataset_ids: List[str]) -> Dict[str, Any]:
        """Populate impact assessments for multiple datasets in batch."""
        url = f"{self.base_url}/datasets/populate-impacts"
        
        log.info(f"Populating impacts for {len(dataset_ids)} datasets")
        response = requests.post(
            url, 
            headers=self.headers,
            json=dataset_ids
        )
        response.raise_for_status()
        result = response.json()
        
        log.info(f"Batch population initiated: {result.get('message')}")
        return result
    
    async def get_datasets_by_impact(self, impact_level: str) -> List[str]:
        """Get list of datasets with a specific impact level."""
        url = f"{self.base_url}/datasets/by-impact/{impact_level}"
        
        log.info(f"Getting datasets with impact level: {impact_level}")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        datasets = response.json()
        
        log.info(f"Found {len(datasets)} datasets with impact level {impact_level}")
        return datasets
    
    async def is_assessment_stale(self, dataset_id: str, days: int = 7) -> bool:
        """Check if a dataset's impact assessment is stale."""
        impact = await self.get_dataset_impact(dataset_id)
        
        assessment_method = impact.get("assessment_method", "")
        if not assessment_method:
            return True
        
        is_stale = "calculated" in assessment_method
        
        status = "stale" if is_stale else "fresh"
        log.info(f"Dataset {dataset_id} assessment is {status}")
        return is_stale


async def run_tests():
    """Run all impact assessment tests."""
    client = ImpactTestClient(API_BASE_URL)
    
    log.info("\n----- STEP 1: Retrieving datasets from API -----")
    initial_datasets = await client.get_datasets(limit=15)
    if not initial_datasets:
        log.error("Failed to retrieve datasets, cannot proceed with tests")
        return
    
    dataset_ids = [d.get("id") for d in initial_datasets if d.get("id")]
    log.info(f"Retrieved {len(dataset_ids)} dataset IDs for testing")
    
    log.info("\n----- TEST: Listing datasets with impact assessment -----")
    datasets_with_impact = await client.get_datasets(limit=5, with_impact=True)
    for dataset in datasets_with_impact:
        impact = dataset.get("impact_assessment", {})
        impact_level = dataset.get("impact_level", "unknown")
        log.info(f"Dataset: {dataset.get('id')}, Impact: {impact_level}")
        if impact:
            log.info(f"  Method: {impact.get('method')}")
            log.info(f"  Metrics: {json.dumps(impact.get('metrics', {}))}")
    
    log.info("\n----- TEST: Getting impact for specific datasets -----")
    test_datasets = random.sample(dataset_ids, min(3, len(dataset_ids)))
    
    for dataset_id in test_datasets:
        impact = await client.get_dataset_impact(dataset_id)
        log.info(f"Dataset: {dataset_id}")
        log.info(f"  Level: {impact.get('impact_level')}")
        log.info(f"  Method: {impact.get('assessment_method')}")
        log.info(f"  Size: {impact.get('metrics', {}).get('size_bytes')} bytes")
        log.info(f"  Downloads: {impact.get('metrics', {}).get('downloads')}")
        log.info(f"  Likes: {impact.get('metrics', {}).get('likes')}")
        
        # Now force a refresh to test saving to database
        log.info(f"Forcing refresh for {dataset_id}...")
        refreshed_impact = await client.get_dataset_impact(dataset_id, force_refresh=True)
        log.info(f"  Refreshed level: {refreshed_impact.get('impact_level')}")
    
    # 4. Test batch population of impacts
    log.info("\n----- TEST: Batch population of impacts -----")
    # Select another subset for batch testing
    batch_datasets = random.sample(
        [d for d in dataset_ids if d not in test_datasets], 
        min(3, len(dataset_ids) - len(test_datasets))
    )
    
    if batch_datasets:
        batch_result = await client.populate_impacts(batch_datasets)
        log.info(f"Batch population result: {batch_result}")
        
        # Wait a bit for background processing
        log.info("Waiting for background processing...")
        await asyncio.sleep(5)
    else:
        log.warning("Not enough datasets for batch testing")
    
    # 5. Test listing datasets by impact level
    log.info("\n----- TEST: Listing datasets by impact level -----")
    for level in ["low", "medium", "high"]:
        datasets = await client.get_datasets_by_impact(level)
        log.info(f"Datasets with impact level '{level}': {datasets}")
    
    # 6. Test checking if assessment is stale
    log.info("\n----- TEST: Checking if assessment is stale -----")
    for dataset_id in test_datasets[:2]:
        is_stale = await client.is_assessment_stale(dataset_id)
        log.info(f"Dataset {dataset_id} assessment is stale: {is_stale}")
    
    log.info("\nAll tests completed!")


if __name__ == "__main__":
    asyncio.run(run_tests())
