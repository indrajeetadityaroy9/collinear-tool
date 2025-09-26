# Collinear Data Tool API

A FastAPI service that catalogs Hugging Face datasets, adds impact assessments, and provides a searchable REST API for dataset discovery. Built with Redis caching, Celery background processing, and comprehensive monitoring capabilities.

## Core Features

### **Dataset Catalog Management**
- Fetches complete Hugging Face dataset registry (hundreds of thousands of datasets)
- Enriches datasets with impact assessments based on size, downloads, and likes
- Classifies datasets as high/medium/low/not_available impact with detailed metrics
- Provides paginated, searchable, sortable access to the entire catalog

### **Search & Discovery**
- **Text Search**: Fast HSCAN-based search across dataset IDs, names, and descriptions
- **Multi-field Sorting**: Efficient sorting by downloads, likes, size with pre-computed indexes
- **Pagination**: Stable ordering with offset/limit support
- **Combined Operations**: Search + sort + pagination in unified queries

### **Dataset Metadata Services**
- **Commit History**: Version tracking for dataset updates
- **File Listings**: Complete file inventories for each dataset
- **Download URLs**: Secure, direct download links for dataset files
- **Real-time Metadata**: Live integration with Hugging Face Hub API

### **Caching**
Redis powers a multi-layer caching system:
- **Sorted Sets** (`hf:datasets:all:zset`): Stable pagination ordering
- **Hash Storage** (`hf:datasets:all:hash`): Enriched dataset documents
- **Sort Indexes** (`:by_downloads`, `:by_likes`, `:by_size_bytes`): Pre-computed sorting
- **Metadata Cache** (`hf:datasets:meta`): Cache status and statistics
- **4GB Memory Pool**: Optimized for dataset cataloging workload

### **Background Processing**
Celery workers handle intensive operations asynchronously:
- **Full Cache Refresh** (`refresh_hf_datasets_full_cache`): Complete dataset synchronization
- **Batch Processing** (`fetch_datasets_page`): Efficient page-wise updates
- **Scheduled Sync**: Automatic hourly cache refreshes
- **Task Deduplication**: Prevents overlapping refresh operations
- **Progress Tracking**: Real-time monitoring of long-running tasks

### **Monitoring**
Observability with Prometheus integration:
- **HTTP Metrics**: Request/response tracking with status codes
- **Redis Operations**: Cache hit/miss ratios and operation timing
- **Memory Profiling**: Automatic garbage collection and memory limits
- **Celery Monitoring**: Task duration, retries, and failure tracking
- **Custom Metrics**: Dataset processing performance and cache efficiency

### **Impact Assessment**
Automated classification system with configurable thresholds:
- **Size-based**: 100MB (low) / 1GB (medium) / 10GB (high) thresholds
- **Popularity-based**: Download and like count analysis
- **Metadata Enrichment**: Detailed assessment reports with metrics and methods
- **Real-time Calculation**: Dynamic impact scoring during cache population

