import asyncio
import logging
import time
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, rate=10, per=1.0):
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()
    async def acquire(self):
        async with self._lock:
            current = time.monotonic()
            time_passed = current - self.last_check
            self.last_check = current
            self.allowance += time_passed * (self.rate / self.per)
            if self.allowance > self.rate:
                self.allowance = self.rate
            if self.allowance < 1.0:
                sleep_time = (1.0 - self.allowance) * (self.per / self.rate)
                log.debug(f"Rate limit reached, waiting {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0


class PooledHTTPClient:
    def __init__(self, base_url=None, headers=None, rate_limit=30, rate_period=1.0, max_connections=10, max_keepalive_connections=5, keepalive_expiry=30.0, timeout=30.0):
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = timeout
        self.rate_limiter = RateLimiter(rate=rate_limit, per=rate_period) if rate_limit else None
        limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry
        )
        self.client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            limits=limits,
            timeout=httpx.Timeout(timeout),
            http2=True,
            follow_redirects=True
        )
    
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get(self, url, params=None, headers=None, timeout=None):
        if self.rate_limiter:
            await self.rate_limiter.acquire()
        request_headers = {**self.headers}
        if headers:
            request_headers.update(headers)
        response = await self.client.get(
            url,
            params=params,
            headers=request_headers,
            timeout=timeout or self.timeout
        )
        response.raise_for_status()
        return response

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def post(self, url, json=None, data=None, headers=None, timeout=None):
        if self.rate_limiter:
            await self.rate_limiter.acquire()
        request_headers = {**self.headers}
        if headers:
            request_headers.update(headers)
        response = await self.client.post(
            url,
            json=json,
            data=data,
            headers=request_headers,
            timeout=timeout or self.timeout
        )
        response.raise_for_status()
        return response


_hf_client = None


async def get_hf_client(token=None):
    global _hf_client
    if _hf_client is None:
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        _hf_client = PooledHTTPClient(
            base_url='https://huggingface.co/api',
            headers=headers,
            rate_limit=30,
            rate_period=1.0,
            max_connections=20,
            max_keepalive_connections=10,
            timeout=120.0
        )
    return _hf_client


async def close_hf_client():
    global _hf_client
    if _hf_client:
        await _hf_client.close()
        _hf_client = None