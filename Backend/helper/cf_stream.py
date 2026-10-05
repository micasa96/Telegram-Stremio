import asyncio
import hashlib
import hmac
import random
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

import httpx

from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

#----- Signed Cloudflare links stay valid this long. The Worker can't read the token DB, so a
#----- revoked or expired token keeps streaming on Cloudflare until its links run out.
LINK_TTL = 48 * 3600

#----- Max clock skew accepted on requests signed by the Worker
REQUEST_MAX_AGE = 300


class WorkerStats:
    """Statistics and health tracking for a single Cloudflare Worker."""
    
    def __init__(self, url: str, secret: str):
        self.url = url
        self.secret = secret
        self.total_streams = 0
        self.failed_checks = 0
        self.is_healthy = True
        self.last_check_time = 0
        self.response_times = deque(maxlen=20)
        self.consecutive_failures = 0
        
    @property
    def avg_response_time(self) -> float:
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)
    
    def record_success(self, response_time_ms: float):
        self.response_times.append(response_time_ms)
        self.consecutive_failures = 0
        if not self.is_healthy:
            LOGGER.info(f"[CF-MULTI] Worker {self.url} is now healthy")
            self.is_healthy = True
    
    def record_failure(self):
        self.failed_checks += 1
        self.consecutive_failures += 1
        if self.consecutive_failures >= 3 and self.is_healthy:
            LOGGER.warning(f"[CF-MULTI] Worker {self.url} marked unhealthy after {self.consecutive_failures} failures")
            self.is_healthy = False
    
    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "total_streams": self.total_streams,
            "failed_checks": self.failed_checks,
            "avg_response_time": round(self.avg_response_time, 2),
            "is_healthy": self.is_healthy,
            "consecutive_failures": self.consecutive_failures,
            "last_check": self.last_check_time,
        }


class CloudflareWorkerManager:
    """
    Manages multiple Cloudflare Workers with load balancing and health monitoring.
    Distributes streams across workers for optimal performance.
    """
    
    def __init__(self):
        self._workers: Dict[str, WorkerStats] = {}
        self._rr_counter = 0
        self._health_check_task: Optional[asyncio.Task] = None
        self._health_check_interval = 45  # seconds
        self._strategy = "round-robin"  # "round-robin", "least-loaded", or "random"
        
    def initialize(self, workers: List[Tuple[str, str]]):
        """
        Initialize workers with (url, secret) pairs.
        
        Args:
            workers: List of (worker_url, shared_secret) tuples
        """
        # Remove workers no longer in config
        current_urls = {url for url, _ in workers}
        for url in list(self._workers.keys()):
            if url not in current_urls:
                LOGGER.info(f"[CF-MULTI] Removing worker: {url}")
                del self._workers[url]
        
        # Add or update workers
        for url, secret in workers:
            if url and secret:
                if url not in self._workers:
                    LOGGER.info(f"[CF-MULTI] Adding worker: {url}")
                    self._workers[url] = WorkerStats(url, secret)
                else:
                    # Update secret if changed
                    self._workers[url].secret = secret
        
        # Start health check if not running
        if self._workers and not self._health_check_task:
            self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        LOGGER.info(f"[CF-MULTI] Manager initialized with {len(self._workers)} workers")
    
    def set_strategy(self, strategy: str):
        """Set load balancing strategy: round-robin, least-loaded, or random."""
        if strategy in ("round-robin", "least-loaded", "random"):
            self._strategy = strategy
            LOGGER.info(f"[CF-MULTI] Load balancing strategy: {strategy}")
    
    def get_worker_count(self) -> int:
        return len(self._workers)
    
    def get_healthy_worker_count(self) -> int:
        return sum(1 for w in self._workers.values() if w.is_healthy)
    
    def _get_healthy_workers(self) -> List[str]:
        return [url for url, stats in self._workers.items() if stats.is_healthy]
    
    def select_worker(self) -> Optional[Tuple[str, str]]:
        """
        Select a worker based on configured strategy.
        
        Returns:
            (worker_url, secret) tuple or None if no workers available
        """
        healthy = self._get_healthy_workers()
        
        if not healthy:
            # No healthy workers, try first available as fallback
            if self._workers:
                LOGGER.warning("[CF-MULTI] No healthy workers, using first available")
                url = list(self._workers.keys())[0]
                stats = self._workers[url]
                return (url, stats.secret)
            return None
        
        # Select based on strategy
        if self._strategy == "least-loaded":
            selected = min(healthy, key=lambda url: self._workers[url].total_streams)
        elif self._strategy == "random":
            selected = random.choice(healthy)
        else:  # round-robin (default)
            selected = healthy[self._rr_counter % len(healthy)]
            self._rr_counter = (self._rr_counter + 1) % max(len(healthy), 1)
        
        stats = self._workers[selected]
        stats.total_streams += 1
        return (selected, stats.secret)
    
    async def _check_worker_health(self, url: str) -> bool:
        """Check if a worker is healthy."""
        stats = self._workers.get(url)
        if not stats:
            return False
        
        # Try to ping the worker's health endpoint
        ts, body = str(int(time.time())), "{}"
        headers = {
            "x-cf-time": ts,
            "x-cf-sig": _hmac(stats.secret, f"{ts}\nGET /health\n{body}"),
            "content-type": "application/json"
        }
        
        try:
            start_time = time.time()
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Try health endpoint first, fallback to root
                try:
                    response = await client.get(f"{url}/health", headers=headers)
                except:
                    response = await client.get(url)
                
                response_time = (time.time() - start_time) * 1000
                
                if response.status_code < 500:
                    stats.record_success(response_time)
                    stats.last_check_time = time.time()
                    return True
                else:
                    stats.record_failure()
                    stats.last_check_time = time.time()
                    return False
                    
        except Exception as e:
            LOGGER.debug(f"[CF-MULTI] Health check failed for {url}: {e}")
            stats.record_failure()
            stats.last_check_time = time.time()
            return False
    
    async def _health_check_loop(self):
        """Background task for periodic health checks."""
        LOGGER.info("[CF-MULTI] Health check loop started")
        
        while self._workers:
            try:
                await asyncio.sleep(self._health_check_interval)
                
                tasks = [self._check_worker_health(url) for url in self._workers.keys()]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                healthy = self.get_healthy_worker_count()
                total = self.get_worker_count()
                LOGGER.info(f"[CF-MULTI] Health check: {healthy}/{total} healthy workers")
                
            except Exception as e:
                LOGGER.error(f"[CF-MULTI] Health check error: {e}")
        
        LOGGER.info("[CF-MULTI] Health check loop stopped")
        self._health_check_task = None
    
    def get_stats(self) -> List[dict]:
        """Get statistics for all workers."""
        return [stats.to_dict() for stats in self._workers.values()]
    
    def get_all_worker_urls(self, token: str, file_id: str, name: str) -> List[dict]:
        """
        Generate stream URLs for all healthy workers.
        Useful for providing multiple stream options to users.
        """
        results = []
        healthy = self._get_healthy_workers()
        
        for i, url in enumerate(healthy, 1):
            stats = self._workers[url]
            exp = int(time.time()) + LINK_TTL
            sig = _hmac(stats.secret, f"/dl/{token}/{file_id}:{exp}")[:32]
            stream_url = f"{url}/dl/{token}/{file_id}/{name}?e={exp}&s={sig}"
            
            # Extract friendly name from URL
            worker_name = url.replace("https://", "").replace("http://", "").split(".")[0]
            
            results.append({
                "url": stream_url,
                "worker_name": f"CF Worker {i}",
                "worker_host": worker_name,
                "avg_response": round(stats.avg_response_time, 2),
            })
        
        return results
    
    def shutdown(self):
        """Stop health check task."""
        if self._health_check_task:
            self._health_check_task.cancel()
            self._health_check_task = None


# Global manager instance
_cf_manager: Optional[CloudflareWorkerManager] = None


def get_cf_manager() -> CloudflareWorkerManager:
    """Get or create the global CloudflareWorkerManager."""
    global _cf_manager
    if _cf_manager is None:
        _cf_manager = CloudflareWorkerManager()
    return _cf_manager


def cf_enabled() -> bool:
    """Check if Cloudflare streaming is enabled (legacy + multi-worker support)."""
    s = SettingsManager.current()
    
    # Check multi-worker mode
    if s.cf_stream_mode != "off" and s.cf_workers:
        return True
    
    # Check legacy single worker mode
    return s.cf_stream_mode != "off" and bool(s.cf_stream_url and s.cf_stream_secret)


def _hmac(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def cf_stream_url(token: str, file_id: str, name: str) -> str:
    """
    Generate a Cloudflare Worker stream URL with automatic load balancing.
    Supports both multi-worker and legacy single-worker configurations.
    """
    s = SettingsManager.current()
    
    # Try multi-worker mode first
    if s.cf_workers:
        manager = get_cf_manager()
        worker_info = manager.select_worker()
        if worker_info:
            worker_url, secret = worker_info
            exp = int(time.time()) + LINK_TTL
            sig = _hmac(secret, f"/dl/{token}/{file_id}:{exp}")[:32]
            return f"{worker_url}/dl/{token}/{file_id}/{name}?e={exp}&s={sig}"
    
    # Fallback to legacy single worker
    if s.cf_stream_url and s.cf_stream_secret:
        exp = int(time.time()) + LINK_TTL
        sig = _hmac(s.cf_stream_secret, f"/dl/{token}/{file_id}:{exp}")[:32]
        return f"{s.cf_stream_url}/dl/{token}/{file_id}/{name}?e={exp}&s={sig}"
    
    return ""


def cf_stream_urls_all(token: str, file_id: str, name: str) -> List[dict]:
    """
    Generate stream URLs for ALL available workers.
    Returns list of dicts with url, worker_name, and stats.
    """
    s = SettingsManager.current()
    
    # Multi-worker mode
    if s.cf_workers:
        manager = get_cf_manager()
        return manager.get_all_worker_urls(token, file_id, name)
    
    # Legacy single worker - return single URL
    if s.cf_stream_url and s.cf_stream_secret:
        exp = int(time.time()) + LINK_TTL
        sig = _hmac(s.cf_stream_secret, f"/dl/{token}/{file_id}:{exp}")[:32]
        url = f"{s.cf_stream_url}/dl/{token}/{file_id}/{name}?e={exp}&s={sig}"
        return [{
            "url": url,
            "worker_name": "CF Worker",
            "worker_host": s.cf_stream_url.replace("https://", "").replace("http://", ""),
            "avg_response": 0,
        }]
    
    return []


#----- Check a Worker -> app request signed as HMAC(secret, "{ts}\n{METHOD} {path}\n{body}")
def verify_worker_request(method: str, path: str, body: bytes, ts: str, sig: str) -> bool:
    """Verify request from any configured worker (multi-worker or legacy single)."""
    s = SettingsManager.current()
    
    if not ts or not sig:
        return False
    
    try:
        if abs(time.time() - int(ts)) > REQUEST_MAX_AGE:
            return False
    except ValueError:
        return False
    
    message = f"{ts}\n{method} {path}\n{body.decode('utf-8', 'replace')}"
    
    # Check against all configured worker secrets
    secrets_to_check = []
    
    # Multi-worker secrets
    if s.cf_workers:
        secrets_to_check.extend([secret for _, secret in s.cf_workers])
    
    # Legacy single worker secret
    if s.cf_stream_secret:
        secrets_to_check.append(s.cf_stream_secret)
    
    # Verify against any valid secret
    for secret in secrets_to_check:
        if secret:
            expected = _hmac(secret, message)
            if hmac.compare_digest(expected, sig):
                return True
    
    return False


async def notify_worker(worker_url: str, secret: str) -> bool:
    """Notify a specific worker to reload config."""
    ts, body = str(int(time.time())), "{}"
    headers = {
        "x-cf-time": ts,
        "x-cf-sig": _hmac(secret, f"{ts}\nPOST /api/sync\n{body}"),
        "content-type": "application/json"
    }
    
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(f"{worker_url}/api/sync", content=body, headers=headers)
        if r.status_code != 200:
            LOGGER.warning(f"[CF] Worker sync failed for {worker_url}: HTTP {r.status_code}")
            return False
        LOGGER.info(f"[CF] Worker synced successfully: {worker_url}")
        return True
    except Exception as e:
        LOGGER.warning(f"[CF] Worker sync failed for {worker_url}: {e}")
        return False


async def notify_all_workers() -> None:
    """Notify all configured workers to reload tokens/session."""
    if not cf_enabled():
        return
    
    s = SettingsManager.current()
    tasks = []
    
    # Notify multi-workers
    if s.cf_workers:
        for url, secret in s.cf_workers:
            if url and secret:
                tasks.append(notify_worker(url, secret))
    
    # Notify legacy single worker
    if s.cf_stream_url and s.cf_stream_secret:
        tasks.append(notify_worker(s.cf_stream_url, s.cf_stream_secret))
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        LOGGER.info(f"[CF] Notified {success_count}/{len(tasks)} workers successfully")


#----- Fire-and-forget: never delays the save or login that triggered it
def sync_worker_soon() -> None:
    asyncio.create_task(notify_all_workers())
