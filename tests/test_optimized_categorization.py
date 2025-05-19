#!/usr/bin/env python3
"""
Test script for optimized dataset categorization functionality.

This script tests:
1. Performance improvement with Redis caching
2. Batch processing with worker queues
3. Horizontal scaling with multiple workers
"""

import asyncio
import argparse
import logging
import time
import uuid
import sys
import random
import json
import os
from datetime import datetime
import statistics
import pytest

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("perf-test")

# Default configuration
# Use API_URL from environment variable if available, otherwise default to localhost
DEFAULT_API_URL = os.environ.get("API_URL", "http://localhost:8000/api")
DEFAULT_REQUEST_COUNT = 50
DEFAULT_CONCURRENCY = 10
DEFAULT_BATCH_SIZE = 25

class PerformanceTestClient:
    """Client to test dataset impact assessment performance."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.headers = {"Content-Type": "application/json"}
        self.timings = []
    
    async def measure_perf(self, endpoint: str, use_cache: bool = True) -> float:
        """Measure performance of API endpoint, returns response time in seconds."""
        url = f"{self.base_url}/{endpoint}"
        
        # Fix how we append query parameters
        if not use_cache:
            # Check if the URL already has query parameters
            if '?' in url:
                url += f"&skip_cache=true&timestamp={int(time.time())}"  # Ensure unique request
            else:
                url += f"?skip_cache=true&timestamp={int(time.time())}"  # Ensure unique request
            
        start_time = time.time()
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
        duration = time.time() - start_time
        self.timings.append(duration)
        return duration
    
    async def test_endpoint_performance(self, endpoint: str, iterations: int, use_cache: bool) -> dict:
        """Test performance of an endpoint multiple times."""
        self.timings = []
        total_time = 0
        
        for i in range(iterations):
            duration = await self.measure_perf(endpoint, use_cache)
            total_time += duration
            log.debug(f"Request {i+1}/{iterations}: {duration:.4f}s")
        
        # Calculate statistics
        avg_time = total_time / iterations
        min_time = min(self.timings)
        max_time = max(self.timings)
        median_time = statistics.median(self.timings)
        p95_time = sorted(self.timings)[int(iterations * 0.95)]
        
        return {
            "avg_time": avg_time,
            "min_time": min_time,
            "max_time": max_time,
            "median_time": median_time,
            "p95_time": p95_time,
            "total_time": total_time,
            "samples": iterations
        }
    
    async def compare_cached_vs_uncached(self, endpoint: str, iterations: int) -> dict:
        """Compare performance with and without caching."""
        log.info(f"Testing uncached performance for {endpoint}")
        uncached = await self.test_endpoint_performance(endpoint, iterations, False)
        
        log.info(f"Testing cached performance for {endpoint}")
        cached = await self.test_endpoint_performance(endpoint, iterations, True)
        
        # Calculate improvement
        avg_improvement = (uncached["avg_time"] - cached["avg_time"]) / uncached["avg_time"] * 100
        p95_improvement = (uncached["p95_time"] - cached["p95_time"]) / uncached["p95_time"] * 100
        
        return {
            "uncached": uncached,
            "cached": cached,
            "avg_improvement_pct": avg_improvement,
            "p95_improvement_pct": p95_improvement
        }
    
    async def get_datasets(self, limit: int = 20) -> list:
        """Get a list of datasets to use for testing."""
        url = f"{self.base_url}/datasets?limit={limit}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            datasets = response.json()
        
        return datasets
    
    async def process_dataset_batch(self, dataset_ids: list) -> dict:
        """Process a batch of datasets and wait for completion."""
        url = f"{self.base_url}/datasets/populate-impacts"
        
        # Submit batch job
        start_time = time.time()
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, 
                headers=self.headers,
                json=dataset_ids
            )
            response.raise_for_status()
            result = response.json()
        
        batch_id = result.get("batch_id")
        if not batch_id:
            log.error(f"No batch ID returned: {result}")
            return {"status": "error", "message": "No batch ID returned"}
        
        log.info(f"Batch job {batch_id} submitted with {len(dataset_ids)} datasets")
        
        # Poll for completion
        status_url = f"{self.base_url}/datasets/batch/{batch_id}"
        complete = False
        max_polls = 60
        polls = 0
        
        while not complete and polls < max_polls:
            await asyncio.sleep(1)
            polls += 1
            
            async with httpx.AsyncClient() as client:
                response = await client.get(status_url, headers=self.headers)
                response.raise_for_status()
                status = response.json()
            
            if status.get("status") == "complete":
                complete = True
                log.info(f"Batch job {batch_id} completed in {time.time() - start_time:.2f}s")
            elif status.get("status") == "error":
                log.error(f"Batch job {batch_id} failed: {status}")
                return status
            else:
                log.debug(f"Batch job {batch_id} progress: {status.get('percent', 0)}%")
        
        if not complete:
            log.warning(f"Batch job {batch_id} did not complete within timeout")
            return {"status": "timeout", "batch_id": batch_id}
        
        duration = time.time() - start_time
        return {
            "status": "complete", 
            "batch_id": batch_id,
            "datasets": len(dataset_ids),
            "duration": duration,
            "datasets_per_second": len(dataset_ids) / duration
        }

    async def test_multiple_impact_requests(self, dataset_ids: list, concurrency: int) -> dict:
        """Test performance of multiple concurrent impact requests."""
        start_time = time.time()
        self.timings = []
        
        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_with_semaphore(dataset_id):
            async with semaphore:
                endpoint = f"datasets/{dataset_id}/impact"
                await self.measure_perf(endpoint)
        
        # Create tasks for all datasets
        tasks = [process_with_semaphore(dataset_id) for dataset_id in dataset_ids]
        
        # Run all tasks
        await asyncio.gather(*tasks)
        
        duration = time.time() - start_time
        return {
            "datasets": len(dataset_ids),
            "concurrency": concurrency,
            "total_time": duration,
            "avg_time": statistics.mean(self.timings),
            "median_time": statistics.median(self.timings),
            "p95_time": sorted(self.timings)[int(len(self.timings) * 0.95)],
            "datasets_per_second": len(dataset_ids) / duration
        }
        
async def run_tests(args):
    """Run all performance tests based on provided arguments."""
    client = PerformanceTestClient(args.api_url)
    
    # Test case 1: Compare cached vs uncached basic mock endpoint
    log.info("\n===== TEST 1: Caching Performance for Mock Endpoint =====")
    mock_result = await client.compare_cached_vs_uncached(
        "health/mock-test", 
        args.requests
    )
    log.info(f"Cached vs Uncached Mock (improvement: {mock_result['avg_improvement_pct']:.1f}%):")
    log.info(f"  Uncached: {mock_result['uncached']['avg_time']:.4f}s avg, {mock_result['uncached']['p95_time']:.4f}s p95")
    log.info(f"  Cached:   {mock_result['cached']['avg_time']:.4f}s avg, {mock_result['cached']['p95_time']:.4f}s p95")
    
    # Test case 2: Compare cached vs uncached dataset impact
    log.info(f"\n===== TEST 2: Caching Performance for Mock Dataset Impact =====")
    impact_result = await client.compare_cached_vs_uncached(
        "health/mock-dataset-impact?dataset_id=test-123", 
        args.requests
    )
    log.info(f"Cached vs Uncached Impact Assessment (improvement: {impact_result['avg_improvement_pct']:.1f}%):")
    log.info(f"  Uncached: {impact_result['uncached']['avg_time']:.4f}s avg, {impact_result['uncached']['p95_time']:.4f}s p95")
    log.info(f"  Cached:   {impact_result['cached']['avg_time']:.4f}s avg, {impact_result['cached']['p95_time']:.4f}s p95")
    
    # Get datasets for remaining tests
    datasets = await client.get_datasets(limit=50)
    if isinstance(datasets, dict) and "items" in datasets:
        datasets = datasets["items"]
    dataset_ids = [d.get("id") for d in datasets if isinstance(d, dict) and d.get("id")]
    log.info(f"Retrieved {len(dataset_ids)} dataset IDs for testing")
    
    # Test case 3: Compare cached vs uncached single dataset impact
    if dataset_ids:
        test_dataset = dataset_ids[0]
        log.info(f"\n===== TEST 3: Caching Performance for Single Dataset Impact ({test_dataset}) =====")
        impact_result = await client.compare_cached_vs_uncached(
            f"datasets/{test_dataset}/impact", 
            args.requests
        )
        log.info(f"Cached vs Uncached Impact Assessment (improvement: {impact_result['avg_improvement_pct']:.1f}%):")
        log.info(f"  Uncached: {impact_result['uncached']['avg_time']:.4f}s avg, {impact_result['uncached']['p95_time']:.4f}s p95")
        log.info(f"  Cached:   {impact_result['cached']['avg_time']:.4f}s avg, {impact_result['cached']['p95_time']:.4f}s p95")
    
    # Test case 4: Batch processing performance
    if len(dataset_ids) >= args.batch_size:
        batch_dataset_ids = random.sample(dataset_ids, args.batch_size)
        log.info(f"\n===== TEST 4: Batch Processing Performance ({args.batch_size} datasets) =====")
        batch_result = await client.process_dataset_batch(batch_dataset_ids)
        if batch_result.get("status") == "complete":
            log.info(f"Batch processing completed in {batch_result['duration']:.2f}s")
            log.info(f"Processing rate: {batch_result['datasets_per_second']:.2f} datasets/second")
        else:
            log.error(f"Batch processing failed or timed out: {batch_result}")
    
    # Test case 5: Multiple concurrent impact requests
    if len(dataset_ids) >= args.concurrency:
        test_dataset_ids = random.sample(dataset_ids, args.concurrency)
        log.info(f"\n===== TEST 5: Concurrent Impact Requests ({args.concurrency} concurrent) =====")
        concurrent_result = await client.test_multiple_impact_requests(
            test_dataset_ids, 
            args.concurrency
        )
        log.info(f"Concurrent processing completed in {concurrent_result['total_time']:.2f}s")
        log.info(f"Average request time: {concurrent_result['avg_time']:.4f}s")
        log.info(f"Median request time: {concurrent_result['median_time']:.4f}s")
        log.info(f"P95 request time: {concurrent_result['p95_time']:.4f}s")
        log.info(f"Processing rate: {concurrent_result['datasets_per_second']:.2f} datasets/second")
    
    log.info("\nAll tests completed!")

    # Print results instead of saving to file
    print("\n===== SUMMARY OF PERFORMANCE TESTS =====")
    print("Mock endpoint:", mock_result)
    print("Mock dataset impact:", impact_result)
    if 'batch_result' in locals():
        print("Batch processing:", batch_result)
    if 'concurrent_result' in locals():
        print("Concurrent requests:", concurrent_result)

@pytest.mark.asyncio
async def test_optimized_categorization():
    class Args:
        api_url = DEFAULT_API_URL
        requests = DEFAULT_REQUEST_COUNT
        concurrency = DEFAULT_CONCURRENCY
        batch_size = DEFAULT_BATCH_SIZE
    await run_tests(Args()) 