import time

import aiohttp

from Backend.logger import LOGGER

_CACHE = {}
_CACHE_TTL = 300  # 5 minutos cache de metadata


def _base_url(manifest_url: str) -> str:
    """Strip the trailing /manifest.json so we can call the addon's other routes."""
    url = str(manifest_url or "").strip().rstrip("/")
    if url.endswith("/manifest.json"):
        url = url[: -len("/manifest.json")]
    return url.rstrip("/")


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


async def get_external_meta(manifest_url, media_type, media_id):
    """Proxy de metadata desde addon externo (protocolo Stremio: /meta/{type}/{id}.json)."""
    cache_key = f"meta:{manifest_url}:{media_type}:{media_id}"
    if cache_key in _CACHE and (time.time() - _CACHE[cache_key]["ts"]) < _CACHE_TTL:
        return _CACHE[cache_key]["data"]
    try:
        url = f"{_base_url(manifest_url)}/meta/{media_type}/{media_id}.json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _CACHE[cache_key] = {"data": data, "ts": time.time()}
                    return data
    except Exception as e:
        LOGGER.error(f"External meta fetch error: {media_id} -> {e}")
    return None


async def get_external_catalog(manifest_url, media_type, catalog_id, extra=None):
    """Proxy de catálogo desde addon externo (protocolo Stremio: /catalog/{type}/{id}[/extra].json)."""
    cache_key = f"catalog:{manifest_url}:{media_type}:{catalog_id}:{extra}"
    if cache_key in _CACHE and (time.time() - _CACHE[cache_key]["ts"]) < _CACHE_TTL:
        return _CACHE[cache_key]["data"]
    try:
        url = f"{_base_url(manifest_url)}/catalog/{media_type}/{catalog_id}"
        if extra:
            url += f"/{extra}"
        url += ".json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {"metas": data.get("metas") or []}
                    _CACHE[cache_key] = {"data": result, "ts": time.time()}
                    return result
    except Exception as e:
        LOGGER.error(f"External catalog fetch error: {media_type}/{catalog_id} -> {e}")
    return None


async def get_external_streams(manifest_url, media_type, media_id):
    """Proxy de streams desde addon externo (protocolo Stremio: /stream/{type}/{id}.json).
    No cacheado — siempre fresh. Unwraps 'behaviorHints.notWebReady': true que muchos
    addons ponen por defecto para bloquear la reproducción directa en Stremio.
    """
    try:
        url = f"{_base_url(manifest_url)}/stream/{media_type}/{media_id}.json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    streams = data.get("streams") or []
                    for s in streams:
                        # Quitar notWebReady para permitir playback directo
                        bh = s.get("behaviorHints") or {}
                        if bh.get("notWebReady"):
                            bh.pop("notWebReady", None)
                            s["behaviorHints"] = bh
                    return {"streams": streams}
    except Exception as e:
        LOGGER.error(f"External stream fetch error: {media_id} -> {e}")
    return None
