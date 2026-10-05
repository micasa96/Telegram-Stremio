"""
API routes for Cloudflare Worker statistics and management.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from Backend.helper.cf_stream import get_cf_manager, cf_enabled
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

router = APIRouter(tags=["Cloudflare Workers"])


@router.get("/api/cf/workers/stats")
async def get_workers_stats():
    """Get statistics for all configured Cloudflare Workers."""
    try:
        if not cf_enabled():
            return JSONResponse({
                "enabled": False,
                "message": "Cloudflare Worker streaming is not enabled"
            })
        
        settings = SettingsManager.current()
        manager = get_cf_manager()
        
        return JSONResponse({
            "enabled": True,
            "mode": settings.cf_stream_mode,
            "load_strategy": settings.cf_load_strategy,
            "show_all_workers": settings.show_all_workers,
            "total_workers": manager.get_worker_count(),
            "healthy_workers": manager.get_healthy_worker_count(),
            "workers": manager.get_stats(),
        })
    except Exception as e:
        LOGGER.error(f"[CF-STATS] Error getting worker stats: {e}")
        return JSONResponse({
            "error": str(e)
        }, status_code=500)


@router.get("/api/cf/workers/health")
async def check_workers_health():
    """
    Force a health check on all workers.
    Returns current health status.
    """
    try:
        if not cf_enabled():
            return JSONResponse({
                "enabled": False,
                "message": "Cloudflare Worker streaming is not enabled"
            })
        
        manager = get_cf_manager()
        
        # Get current stats (health checks run automatically in background)
        return JSONResponse({
            "enabled": True,
            "healthy_workers": manager.get_healthy_worker_count(),
            "total_workers": manager.get_worker_count(),
            "workers": [
                {
                    "url": stats["url"],
                    "is_healthy": stats["is_healthy"],
                    "avg_response_time": stats["avg_response_time"],
                    "consecutive_failures": stats["consecutive_failures"],
                }
                for stats in manager.get_stats()
            ]
        })
    except Exception as e:
        LOGGER.error(f"[CF-HEALTH] Error checking worker health: {e}")
        return JSONResponse({
            "error": str(e)
        }, status_code=500)


@router.post("/api/cf/workers/reload")
async def reload_workers():
    """
    Reload worker configuration from settings.
    Useful after manually updating worker URLs/secrets in the database.
    """
    try:
        settings = SettingsManager.current()
        
        if not settings.cf_workers and not (settings.cf_stream_url and settings.cf_stream_secret):
            return JSONResponse({
                "success": False,
                "message": "No workers configured"
            })
        
        manager = get_cf_manager()
        
        # Reinitialize with current settings
        if settings.cf_workers:
            manager.initialize(settings.cf_workers)
            manager.set_strategy(settings.cf_load_strategy)
            
            return JSONResponse({
                "success": True,
                "workers_loaded": len(settings.cf_workers),
                "strategy": settings.cf_load_strategy
            })
        else:
            return JSONResponse({
                "success": True,
                "message": "Using legacy single worker mode",
                "worker_url": settings.cf_stream_url
            })
            
    except Exception as e:
        LOGGER.error(f"[CF-RELOAD] Error reloading workers: {e}")
        return JSONResponse({
            "error": str(e)
        }, status_code=500)
