import asyncio, time, aiohttp
from Backend.logger import LOGGER

_CACHE = {}
_CACHE_TTL = 300  # 5 minutos cache de metadata

async def get_external_manifest(manifest_url):
    """Fetch y cache del manifest externo."""
    cache_key = f"manifest:{manifest_url}"
    if cache_key in _CACHE and (time.time() - _CACHE[cache_key]["ts"]) < _CACHE_TTL:
        return _CACHE[cache_key]["data"]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(manifest_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _CACHE[cache_key] = {"data": data, "ts": time.time()}
                    return data
    except Exception as e:
        LOGGER.error(f"External manifest fetch error: {manifest_url} -> {e}")
    return None

async def get_external_meta(manifest_url, media_id):
    """Proxy de metadata desde addon externo."""
    cache_key = f"meta:{manifest_url}:{media_id}"
    if cache_key in _CACHE and (time.time() - _CACHE[cache_key]["ts"]) < _CACHE_TTL:
        return _CACHE[cache_key]["data"]
    try:
        base = manifest_url.replace("/manifest.json", "")
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/meta/{media_id}", timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _CACHE[cache_key] = {"data": data, "ts": time.time()}
                    return data
    except Exception as e:
        LOGGER.error(f"External meta fetch error: {media_id} -> {e}")
    return None

async def get_external_streams(manifest_url, media_id):
    """Proxy de streams desde addon externo (no cacheado — siempre fresh)."""
    try:
        base = manifest_url.replace("/manifest.json", "")
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/stream/{media_id}", timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        LOGGER.error(f"External stream fetch error: {media_id} -> {e}")
    return None
