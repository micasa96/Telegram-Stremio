"""Notify a Telegram group/channel when a new title is requested via the
public Request page. Mirrors the pattern used in announcer.py, but fires on
*new* requests (submit_request reason == "created") instead of new uploads.
"""
from asyncio import create_task

from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER
from Backend.pyrofork.bot import StreamBot


def _resolve_chat(value: str):
    value = str(value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return value


def _build_caption(doc: dict) -> str:
    is_tv = doc.get("media_type") == "tv"
    title = doc.get("title") or "Unknown"
    header = f"🆕 <b>Nuevo pedido</b>\n\n{'📺' if is_tv else '🎬'} <b>{title}</b>"
    if doc.get("year"):
        header += f" ({doc['year']})"

    lines = [header, "", f"🗂 <b>Tipo:</b> {'Serie' if is_tv else 'Película'}"]
    lines.append("👤 Pedido por un usuario en la página de Requests.")
    return "\n".join(lines)


def _build_markup(doc: dict):
    rows = []
    tmdb_id = doc.get("tmdb_id")
    if tmdb_id:
        media_path = "tv" if doc.get("media_type") == "tv" else "movie"
        rows.append([InlineKeyboardButton(
            "🔎 Ver en TMDB",
            url=f"https://www.themoviedb.org/{media_path}/{tmdb_id}",
        )])
    base = SettingsManager.current().base_url
    if base:
        rows.append([InlineKeyboardButton("⚙️ Panel de Requests", url=f"{base}/requests")])
    return InlineKeyboardMarkup(rows) if rows else None


async def _notify(doc: dict) -> None:
    settings = SettingsManager.current()
    chat = _resolve_chat(settings.request_notify_channel)
    if not settings.notify_new_requests or chat is None:
        return

    caption = _build_caption(doc)
    poster = doc.get("poster")
    markup = _build_markup(doc)

    try:
        sent = None
        if poster:
            try:
                sent = await StreamBot.send_photo(
                    chat, poster, caption=caption,
                    parse_mode=ParseMode.HTML, reply_markup=markup,
                )
            except FloodWait:
                raise
            except Exception:
                sent = None
        if sent is None:
            await StreamBot.send_message(
                chat, caption, parse_mode=ParseMode.HTML,
                reply_markup=markup, disable_web_page_preview=True,
            )
    except FloodWait as e:
        LOGGER.warning(f"Request notification FloodWait for {e.value}s")
    except Exception as e:
        LOGGER.error(f"Request notification failed for '{doc.get('title')}': {e}")


#----- Fire-and-forget notification for a freshly created request (call once per new title)
def notify_new_request(doc: dict) -> None:
    try:
        create_task(_notify(dict(doc)))
    except RuntimeError:
        LOGGER.warning("notify_new_request called outside an event loop; skipped")
