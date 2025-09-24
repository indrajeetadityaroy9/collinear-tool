# Collinear Data Tool API

A FastAPI service that catalogs Hugging Face datasets, classifies their impact, and exposes the metadata to clients through a REST API. A Redis-backed cache keeps responses fast while Celery workers keep the catalog synchronized with the Hugging Face Hub.

## What the service does
- **Dataset catalog:** Fetches the full Hugging Face dataset registry, annotates each dataset with size, download, like metrics, and derives an impact level (low/medium/high/not available).
- **Searchable API:** Provides `/api/datasets` endpoints for paginated listing, free-text search, and basic sorting. Clients can also fetch commit history, file listings, and download URLs for individual datasets.
- **Operational telemetry:** Returns cache metadata (last refresh, total items, warming/refreshing flags) so the frontend can display loading states.

## Redis integration
Redis is the primary persistence layer for the dataset cache and job coordination:
- A **sorted set** stores dataset IDs with stable ordering for pagination.
- A **hash** holds the enriched dataset documents that the API serves.
- A **stream** captures incremental updates, paving the way for downstream consumers.
- A **base64-encoded gzip blob** mirrors the full catalog for quick bulk reads.
- A dedicated **meta key** records `last_update`, `total_items`, and whether a refresh is in progress; the `/api/datasets/cache-status` endpoint reads from here.

The FastAPI layer uses these structures to serve real-time responses without recomputing dataset metadata. If the cache is empty (e.g., on cold start) the API surfaces a `warming_up` flag so clients can handle empty states.

## Celery workflow
Celery workers handle long-running synchronization with Hugging Face:
- `refresh_hf_datasets_full_cache`: Fetches the entire dataset list, repopulates all Redis structures, and updates the metadata key. The `/api/datasets/refresh-cache` endpoint queues this task asynchronously so the API thread stays responsive.
- `fetch_datasets_page`: Processes a page of datasets, enriching and inserting them into Redis one page at a time. The periodic beat schedule enqueues this task in bulk to keep the cache fresh without downloading the entire catalog every cycle.
- `refresh_hf_datasets_cache_task`: Orchestrates the page-wise refresh by inspecting the total dataset count and dispatching a batch of `fetch_datasets_page` jobs.

Celery connects to the same Redis instance for both the broker and result backend, ensuring task status and cached data share the same infrastructure. Worker logging records successes/failures so operators can monitor refresh health.

