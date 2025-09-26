import gc
import logging
import psutil
import resource
import time
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

log = logging.getLogger(__name__)


class MemoryProfilerMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, memory_limit_mb=500, gc_threshold_mb=300, log_requests=False):
        super().__init__(app)
        self.memory_limit_mb = memory_limit_mb
        self.gc_threshold_mb = gc_threshold_mb
        self.log_requests = log_requests
        self.process = psutil.Process()
    def get_memory_usage(self):
        return self.process.memory_info().rss / 1024 / 1024
    def get_memory_stats(self):
        memory_info = self.process.memory_info()
        memory_percent = self.process.memory_percent()
        virtual_memory = psutil.virtual_memory()
        return {
            'process_rss_mb': memory_info.rss / 1024 / 1024,
            'process_vms_mb': memory_info.vms / 1024 / 1024,
            'process_percent': memory_percent,
            'system_total_mb': virtual_memory.total / 1024 / 1024,
            'system_available_mb': virtual_memory.available / 1024 / 1024,
            'system_percent': virtual_memory.percent
        }
    async def dispatch(self, request, call_next):
        start_time = time.time()
        start_memory = self.get_memory_usage()
        if start_memory > self.memory_limit_mb:
            log.error(f"Memory limit exceeded: {start_memory:.2f}MB > {self.memory_limit_mb}MB")
            gc.collect()
            current_memory = self.get_memory_usage()
            if current_memory > self.memory_limit_mb:
                return Response(
                    content="Service temporarily unavailable due to high memory usage",
                    status_code=503,
                    headers={
                        'X-Memory-Usage-MB': str(current_memory),
                        'X-Memory-Limit-MB': str(self.memory_limit_mb)
                    }
                )
        if start_memory > self.gc_threshold_mb:
            log.info(f"Triggering GC due to memory usage: {start_memory:.2f}MB")
            gc.collect()
            start_memory = self.get_memory_usage()
        response = await call_next(request)
        end_memory = self.get_memory_usage()
        memory_delta = end_memory - start_memory
        process_time = time.time() - start_time
        response.headers['X-Memory-Usage-MB'] = f"{end_memory:.2f}"
        response.headers['X-Memory-Delta-MB'] = f"{memory_delta:.2f}"
        response.headers['X-Process-Time'] = f"{process_time:.3f}"
        if self.log_requests or abs(memory_delta) > 10:
            log.info(
                f"{request.method} {request.url.path} - "
                f"Memory: {end_memory:.2f}MB (Δ{memory_delta:+.2f}MB) - "
                f"Time: {process_time:.3f}s"
            )
        if end_memory > self.gc_threshold_mb:
            log.warning(f"High memory usage after request: {end_memory:.2f}MB")
        return response


class MemoryLimiter:
    def __init__(self, limit_mb):
        self.limit_bytes = limit_mb * 1024 * 1024
    def __enter__(self):
        try:
            resource.setrlimit(
                resource.RLIMIT_AS,
                (self.limit_bytes, self.limit_bytes)
            )
        except Exception as exc:
            log.warning(f"Failed to set memory limit: {exc}")
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            resource.setrlimit(
                resource.RLIMIT_AS,
                (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
            )
        except Exception:
            pass


def get_memory_info():
    process = psutil.Process()
    memory_info = process.memory_info()
    virtual_memory = psutil.virtual_memory()
    gc.collect()
    return {
        'process': {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'percent': process.memory_percent(),
            'num_threads': process.num_threads(),
            'num_fds': process.num_fds() if hasattr(process, 'num_fds') else None
        },
        'system': {
            'total_mb': virtual_memory.total / 1024 / 1024,
            'available_mb': virtual_memory.available / 1024 / 1024,
            'used_mb': virtual_memory.used / 1024 / 1024,
            'percent': virtual_memory.percent
        },
        'gc': {
            'collections': gc.get_count(),
            'objects': len(gc.get_objects()),
            'threshold': gc.get_threshold()
        }
    }