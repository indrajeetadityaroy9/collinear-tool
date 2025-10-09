# Data Tool API

A FastAPI service that implements a Hugging Face datasets registry and provides dataset impact assessment, and exposes the catalog through a searchable, paginated REST API. Redis keeps the catalog warm for millisecond lookups, Celery orchestrates long-running synchronization jobs, and Prometheus-compatible metrics provide insight into performance and reliability.

| Component | Role |
| --- | --- |
| FastAPI (`app/main.py`) | Serves REST endpoints, boots cache warm-up and exposes operational endpoints. |
| Redis | Primary data store for dataset documents, sorted indexes, cache metadata, and task coordination. |
| Celery (`app/tasks/dataset_tasks.py`) | Runs background jobs for full cache refreshes and paginated fetches; scheduled hourly via Celery beat. |
| Hugging Face Hub (`app/integrations/hf_datasets.py`) | Source of truth for dataset metadata, commit history, file listings, and download URLs. |
| Monitoring (`app/metrics/prometheus.py`) | Exposes request, cache, Celery, and memory metrics; `MemoryProfilerMiddleware` enforces per-request limits. |

## Features

- **Dataset Catalog Management** – Fetches the full Hugging Face registry, persists documents and sorted indexes (`downloads`, `likes`, `size_bytes`) in Redis.
- **Impact Scoring** – Applies deterministic size/download/like thresholds to classify datasets as `high`, `medium`, `low`, or `not_available`.
- **Search & Sorting** – Combines hash scans and Redis sorted sets for free-text search, multi-field ordering, and stable pagination.
- **Metadata Services** – On-demand commit history, file inventories, and pre-signed download URLs surfaced through API endpoints.
- **Operational Visibility** – Cache status, memory snapshots, and Prometheus metrics available without leaving the API surface.

## API

| Method & Path | Description |
| --- | --- |
| `GET /` | Basic sanity check endpoint. |
| `GET /api/datasets` | Paginated catalog listing.<br>Query params: `limit` (1–1000), `offset`, `search`, `sort_by` (`downloads`, `likes`, `size_bytes`), `sort_order` (`asc`/`desc`). |
| `GET /api/datasets/cache-status` | Returns `total_items`, `last_update`, cache warm-up flags, and refresh state. |
| `POST /api/datasets/refresh-cache` | Kicks off a full background refresh (requires `HF_API_TOKEN`). |
| `GET /api/datasets/{dataset_id}/commits` | Recent commit history for a dataset repository. |
| `GET /api/datasets/{dataset_id}/files` | Lists files in the dataset repository. |
| `GET /api/datasets/{dataset_id}/file-url?filename=...` | Generates a temporary download URL; optional `revision` query parameter. |
| `GET /api/datasets/meta` | Raw cache metadata hash. |
| `GET /api/datasets/memory-status` | Snapshot of process/system memory including GC stats. |
| `GET /api/datasets/metrics` | Prometheus metrics endpoint. |
