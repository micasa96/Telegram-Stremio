"""
Multi-proxy manager with automatic load balancing and health checks.
Supports round-robin distribution and failover to healthy proxies.
"""

import asyncio
import time
from collections import deque
from typing import Dict, List, Optional
from urllib.parse import quote

import httpx

from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER


class ProxyStats:
    """Statistics and health tracking for a single proxy."""
    
    def __init__(self, url: str):
        self.url = url
        self.total_requests = 0
        self.failed_requests = 0
        self.last_check_time = 0
        self.is_healthy = True
        self.response_times = deque(maxlen=10)  # Keep last 10 response times
        self.consecutive_failures = 0
        
    @property
    def avg_response_time(self) -> float:
        """Average response time in milliseconds."""
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)
    
    @property
    def failure_rate(self) -> float:
        """Percentage of failed requests."""
        if self.total_requests == 0:
            return 0.0
        return (self.failed_requests / self.total_requests) * 100
    
    def record_success(self, response_time_ms: float):
        """Record a successful request."""
        self.total_requests += 1
        self.response_times.append(response_time_ms)
        self.consecutive_failures = 0
        if not self.is_healthy:
            LOGGER.info(f"[PROXY] {self.url} is now healthy")
            self.is_healthy = True
    
    def record_failure(self):
        """Record a failed request."""
        self.total_requests += 1
        self.failed_requests += 1
        self.consecutive_failures += 1
        
        # Mark unhealthy after 3 consecutive failures
        if self.consecutive_failures >= 3 and self.is_healthy:
            LOGGER.warning(f"[PROXY] {self.url} marked as unhealthy after {self.consecutive_failures} failures")
            self.is_healthy = False
    
    def to_dict(self) -> dict:
        """Export stats as dictionary."""
        return {
            "url": self.url,
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
            "failure_rate": round(self.failure_rate, 2),
            "avg_response_time": round(self.avg_response_time, 2),
            "is_healthy": self.is_healthy,
            "consecutive_failures": self.consecutive_failures,
            "last_check": self.last_check_time,
        }


class ProxyManager:
    """
    Manages multiple proxy servers with load balancing and health monitoring.
    
    Strategies:
    - Round-robin: Distributes requests evenly across all healthy proxies
    - Least-loaded: Selects proxy with fewest active requests
    - Health-aware: Automatically excludes unhealthy proxies
    """
    
    def __init__(self):
        self._proxies: Dict[str, ProxyStats] = {}
        self._rr_counter = 0
        self._health_check_task: Optional[asyncio.Task] = None
        self._health_check_interval = 60  # seconds
        self._initialized = False
        
    def initialize(self, proxy_urls: List[str]):
        """Initialize or update the proxy list."""
        # Remove proxies that are no longer in the list
        urls_set = set(proxy_urls)
        for url in list(self._proxies.keys()):
            if url not in urls_set:
                LOGGER.info(f"[PROXY] Removing proxy: {url}")
                del self._proxies[url]
        
        # Add new proxies
        for url in proxy_urls:
            if url and url not in self._proxies:
                LOGGER.info(f"[PROXY] Adding proxy: {url}")
                self._proxies[url] = ProxyStats(url)
        
        # Start health check task if not running
        if self._proxies and not self._health_check_task:
            self._health_check_task = asyncio.create_task(self._health_check_loop())
            
        self._initialized = True
        LOGGER.info(f"[PROXY] Manager initialized with {len(self._proxies)} proxies")
    
    def get_proxy_count(self) -> int:
        """Total number of configured proxies."""
        return len(self._proxies)
    
    def get_healthy_proxy_count(self) -> int:
        """Number of healthy proxies."""
        return sum(1 for p in self._proxies.values() if p.is_healthy)
    
    def _get_healthy_proxies(self) -> List[str]:
        """Get list of healthy proxy URLs."""
        return [url for url, stats in self._proxies.items() if stats.is_healthy]
    
    def select_proxy(self, strategy: str = "round-robin") -> Optional[str]:
        """
        Select a proxy based on the given strategy.
        
        Args:
            strategy: "round-robin" or "least-loaded"
            
        Returns:
            Proxy URL or None if no healthy proxies available
        """
        healthy = self._get_healthy_proxies()
        
        if not healthy:
            # No healthy proxies, try any proxy as last resort
            if self._proxies:
                LOGGER.warning("[PROXY] No healthy proxies, using first available")
                return list(self._proxies.keys())[0]
            return None
        
        if strategy == "least-loaded":
            # Select proxy with lowest request count
            return min(healthy, key=lambda url: self._proxies[url].total_requests)
        
        # Default: round-robin
        selected = healthy[self._rr_counter % len(healthy)]
        self._rr_counter = (self._rr_counter + 1) % max(len(healthy), 1)
        return selected
    
    def build_proxy_url(self, original_url: str, proxy_url: Optional[str] = None) -> Optional[str]:
        """
        Build a proxied URL for the given original URL.
        
        Args:
            original_url: The direct stream URL
            proxy_url: Specific proxy to use, or None to auto-select
            
        Returns:
            Proxied URL or None if no proxies available
        """
        if not proxy_url:
            proxy_url = self.select_proxy()
        
        if not proxy_url:
            return None
        
        settings = SettingsManager.current()
        
        if settings.mediaflow_proxy:
            url = f"{proxy_url.rstrip('/')}/proxy/stream?d={quote(original_url, safe='')}"
            if settings.mediaflow_password:
                url += f"&api_password={quote(settings.mediaflow_password, safe='')}"
            return url
        
        return f"{proxy_url}{original_url}"
    
    async def _check_proxy_health(self, url: str) -> bool:
        """
        Check if a proxy is healthy by making a test request.
        
        Returns:
            True if healthy, False otherwise
        """
        stats = self._proxies.get(url)
        if not stats:
            return False
        
        test_url = f"{url.rstrip('/')}/health" if not SettingsManager.current().mediaflow_proxy else url.rstrip('/')
        
        try:
            start_time = time.time()
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(test_url)
                response_time = (time.time() - start_time) * 1000  # Convert to ms
                
                if response.status_code < 500:  # 2xx, 3xx, 4xx are considered "reachable"
                    stats.record_success(response_time)
                    stats.last_check_time = time.time()
                    return True
                else:
                    stats.record_failure()
                    stats.last_check_time = time.time()
                    return False
                    
        except Exception as e:
            LOGGER.debug(f"[PROXY] Health check failed for {url}: {e}")
            stats.record_failure()
            stats.last_check_time = time.time()
            return False
    
    async def _health_check_loop(self):
        """Background task that periodically checks proxy health."""
        LOGGER.info("[PROXY] Health check loop started")
        
        while self._proxies:
            try:
                await asyncio.sleep(self._health_check_interval)
                
                # Check all proxies
                tasks = [self._check_proxy_health(url) for url in self._proxies.keys()]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                # Log health summary
                healthy_count = self.get_healthy_proxy_count()
                total_count = self.get_proxy_count()
                LOGGER.info(f"[PROXY] Health check complete: {healthy_count}/{total_count} healthy")
                
            except Exception as e:
                LOGGER.error(f"[PROXY] Health check loop error: {e}")
        
        LOGGER.info("[PROXY] Health check loop stopped")
        self._health_check_task = None
    
    def get_stats(self) -> List[dict]:
        """Get statistics for all proxies."""
        return [stats.to_dict() for stats in self._proxies.values()]
    
    def record_request_result(self, proxy_url: str, success: bool, response_time_ms: float = 0):
        """
        Manually record a request result (useful for tracking actual stream requests).
        
        Args:
            proxy_url: The proxy that was used
            success: Whether the request succeeded
            response_time_ms: Response time in milliseconds
        """
        stats = self._proxies.get(proxy_url)
        if stats:
            if success:
                stats.record_success(response_time_ms)
            else:
                stats.record_failure()
    
    def shutdown(self):
        """Stop health check task."""
        if self._health_check_task:
            self._health_check_task.cancel()
            self._health_check_task = None


# Global singleton instance
_proxy_manager: Optional[ProxyManager] = None


def get_proxy_manager() -> ProxyManager:
    """Get or create the global ProxyManager instance."""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager


def initialize_proxy_manager(proxy_urls: List[str]):
    """Initialize the proxy manager with a list of proxy URLs."""
    manager = get_proxy_manager()
    manager.initialize(proxy_urls)


def build_proxy_url(original_url: str) -> Optional[str]:
    """
    Build a proxied URL using automatic proxy selection.
    
    This is a convenience function that uses the global proxy manager.
    """
    manager = get_proxy_manager()
    if manager.get_proxy_count() == 0:
        return None
    return manager.build_proxy_url(original_url)


def get_all_proxy_urls(original_url: str) -> List[dict]:
    """
    Get URLs for all available proxies (for showing multiple stream options).
    
    Returns:
        List of dicts with 'proxy_url' and 'proxy_name' keys
    """
    manager = get_proxy_manager()
    results = []
    
    for i, proxy_url in enumerate(manager._proxies.keys(), 1):
        stats = manager._proxies[proxy_url]
        if stats.is_healthy:
            proxied = manager.build_proxy_url(original_url, proxy_url)
            if proxied:
                # Extract a friendly name from the URL
                proxy_name = proxy_url.replace("http://", "").replace("https://", "").split("/")[0]
                results.append({
                    "proxy_url": proxied,
                    "proxy_name": f"Proxy {i}",
                    "proxy_host": proxy_name,
                })
    
    return results
