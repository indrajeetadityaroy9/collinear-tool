#!/usr/bin/env python3
"""
Dataset Impact Worker Runner

This script starts the dataset impact assessment worker processes.
Run multiple instances of this script to scale out worker capacity.

Usage:
    python workers.py
"""

import os
import sys
import logging
import argparse
from app.workers.dataset_worker import run_worker

if __name__ == "__main__":
    # Configure argument parser
    parser = argparse.ArgumentParser(description="Dataset Impact Worker Runner")
    parser.add_argument("--log-level", 
                     choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                     default="INFO",
                     help="Set the logging level")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    logger = logging.getLogger("worker")
    logger.info("Starting dataset impact worker process")
    
    # Run the worker
    run_worker() 