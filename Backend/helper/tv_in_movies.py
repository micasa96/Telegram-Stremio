"""Detect and move TV shows that were indexed into the movies collection.

A title lands in `movie` when the file name had no SxxExx at index time. This
tool re-resolves each file as a series and moves the document over.
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import Any

from Backend.helper.encrypt import decode_string
from Backend.helper.metadata import metadata
from Backend.helper.pyro import clean_filename
from Backend.logger import LOGGER

EP_RE = re.compile(r"(?i)\bS\d{1,3}\s*E\d{1,4}\b")

_state: dict = {
    "running": False,
    "cancel": False,
    "dry_run": False,
    "total": 0,
    "processed": 0,
    "moved": 0,
    "skipped": 0,
    "failed": 0,
    "files_moved": 0,
    "started_at": 0.0,
    "finished_at": 0.0,
    "error": None,
}


def get_state() -> dict:
    return dict(_state)


def cancel() -> dict:
    if not _state["running"]:
        return {"ok": False, "message": "Nothing is running."}
    _state["cancel"] = True
    return {"ok": True, "message": "Stopping after the current title."}


async def find_candidates(db, limit: int = 0) -> list[dict]:
    """Movie docs whose files look like episodes (SxxExx) or carry seasons."""
    out: list[dict] = []
    for index in range(1, db.current_db_index + 1):
        storage = db.dbs.get(f"storage_{index}")
        if storage is None:
            continue
        async for doc in storage["movie"].find({}):
            names = [q.get("name") or "" for q in (doc.get("telegram") or [])]
            if doc.get("seasons") or any(EP_RE.search(n) for n in names):
                doc["db_index"] = index
                out.append(doc)
                if limit and len(out) >= limit:
                    return out
    return out


async def preview(db, limit: int = 25) -> dict:
    docs = await find_candidates(db)
    return {
        "count": len(docs),
        "sample": [
            {
                "tmdb_id": d.get("tmdb_id"),
                "title": d.get("title") or "",
                "year": d.get("release_year"),
                "db_index": d.get("db_index"),
                "files": [q.get("name") or "" for q in (d.get("telegram") or [])][:4],
            }
            for d in docs[:limit]
        ],
    }


async def _move_doc(db, doc: dict, dry_run: bool) -> int:
    """Re-index the doc's episode files as a series. Returns files inserted."""
    inserted = 0
    for q in doc.get("telegram") or []:
        name = q.get("name") or ""
        if not EP_RE.search(name):
            continue
        try:
            decoded = await decode_string(q.get("id"))
        except Exception:
            continue
        if not isinstance(decoded, dict):
            continue
        chat_id = decoded.get("chat_id")
        msg_id = decoded.get("msg_id")
        if chat_id is None or msg_id is None:
            continue
        try:
            chat_id = int(chat_id)
            msg_id = int(msg_id)
        except (TypeError, ValueError):
            continue

        diag: dict = {}
        info = await metadata(
            clean_filename(name),
            chat_id,
            msg_id,
            quality_override=q.get("quality") or None,
            diag=diag,
            file_name=name,
            media_type="tv",
        )
        if not info:
            continue
        if dry_run:
            inserted += 1
            continue

        status: dict = {}
        ok = await db.insert_media(
            info,
            channel=chat_id,
            msg_id=msg_id,
            size=q.get("size") or "",
            raw_size=0,
            name=name,
            status=status,
        )
        if ok:
            inserted += 1

    if dry_run:
        return inserted
    if not inserted:
        return 0

    storage = db.dbs.get(f"storage_{int(doc.get('db_index') or 1)}")
    if storage is not None:
        await storage["movie"].delete_one({"_id": doc["_id"]})
    return inserted


async def run(db, *, dry_run: bool = False) -> None:
    _state.update({
        "running": True,
        "cancel": False,
        "dry_run": dry_run,
        "total": 0,
        "processed": 0,
        "moved": 0,
        "skipped": 0,
        "failed": 0,
        "files_moved": 0,
        "started_at": time.time(),
        "finished_at": 0.0,
        "error": None,
    })
    try:
        docs = await find_candidates(db)
        _state["total"] = len(docs)
        for doc in docs:
            if _state["cancel"]:
                break
            try:
                files = await _move_doc(db, doc, dry_run)
            except Exception as e:
                _state["failed"] += 1
                LOGGER.warning(f"[TV-in-movies] failed for {doc.get('title')}: {e}")
                _state["processed"] += 1
                continue

            if files:
                _state["moved"] += 1
                _state["files_moved"] += files
            else:
                _state["skipped"] += 1
            _state["processed"] += 1
            await asyncio.sleep(0.05)
    except Exception as e:
        _state["error"] = str(e)
        LOGGER.error(f"[TV-in-movies] crashed: {e}")
    finally:
        _state["running"] = False
        _state["cancel"] = False
        _state["finished_at"] = time.time()
        LOGGER.info(
            f"[TV-in-movies] {'(dry run) ' if dry_run else ''}finished — "
            f"moved {_state['moved']}, skipped {_state['skipped']}, failed {_state['failed']}"
        )


async def start(db, *, dry_run: bool = False) -> dict:
    if _state["running"]:
        return {"ok": False, "message": "Already running.", "status": get_state()}
    asyncio.create_task(run(db, dry_run=dry_run))
    return {"ok": True, "message": "Started in the background.", "status": get_state()}
