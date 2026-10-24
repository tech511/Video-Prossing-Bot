"""
database/user_info.py

MongoDB storage (async, Motor) for the bot:
    - users            : id, names, username, joined_at, last_seen, banned
    - bot_stats        : lifetime task counters (finished / failed / cancelled)

Used by: handle/start.py, handle/stats.py, handle/ban.py, handle/unban.py,
         handle/broadcast.py and choice.py.

Install:  pip install motor pymongo

Environment variables (never hard-code secrets):
    MONGO_URI    mongodb+srv://user:pass@cluster/...
    DB_NAME      (optional) default "VideoBot"
    LOG_CHANNEL  numeric channel ID, e.g. -1001234567890
    OWNER_ID     your Telegram ID (several IDs: comma separated)
                 -> if the variable is missing, `OWNER_ID` from config.py is used
"""

import asyncio
import html
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import (
    ConfigurationError,
    DuplicateKeyError,
    PyMongoError,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
MONGO_URI: str = os.environ.get("MONGO_URI", "").strip()
DB_NAME: str = os.environ.get("DB_NAME", "VideoBot").strip() or "VideoBot"
USERS_COLLECTION = "users"
STATS_COLLECTION = "bot_stats"

STARTED_AT: float = time.time()          # used for the uptime in /stats

_MAX_NAME_LEN = 128
_MAX_USERNAME_LEN = 64
TOUCH_INTERVAL = 600                      # refresh last_seen at most every 10 min / user
_MAX_TOUCH_CACHE = 50_000
_STATS_CACHE_TTL = 5                      # seconds

# choice.py passes the job title; we turn it into a short stable key.
TASK_KINDS = {
    "Creating ZIP": "zip",
    "Trimming media": "trim",
    "Cutting clips": "clips",
    "Merging files": "merge",
    "Extracting audio": "audio",
}
TASK_OUTCOMES = ("finished", "failed", "cancelled")


def _parse_log_channel(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        return int(raw.strip())
    except ValueError:
        logger.error("LOG_CHANNEL must be a numeric ID like -1001234567890.")
        return None


def _load_owner_ids() -> frozenset:
    raw = os.environ.get("OWNER_ID")
    if not raw:
        try:
            from config import OWNER_ID as raw  # type: ignore
        except Exception:
            raw = ""
    ids = {int(x) for x in re.findall(r"\d+", str(raw)) if int(x) > 0}
    if not ids:
        logger.warning("OWNER_ID is not set: owner commands are disabled.")
    return frozenset(ids)


LOG_CHANNEL: Optional[int] = _parse_log_channel(os.environ.get("LOG_CHANNEL"))
OWNER_IDS: frozenset = _load_owner_ids()


def _clean(value: Any, max_len: int) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()[:max_len]
    return value or None


def _valid_id(user_id: Any) -> bool:
    return isinstance(user_id, int) and not isinstance(user_id, bool) and user_id > 0


class UserDatabase:
    """Async wrapper around the bot's MongoDB collections."""

    def __init__(self, uri: str, db_name: str, log_channel: Optional[int]):
        self._log_channel = log_channel
        self._client: Optional[AsyncIOMotorClient] = None
        self._col = None
        self._stats_col = None
        self._indexed = False

        self._touched: dict = {}          # user_id -> monotonic time of last DB write
        self._banned: set = set()         # in-memory ban cache (fast guard)
        self._ban_loaded = False
        self._ban_lock: Optional[asyncio.Lock] = None
        self._stats_cache: Optional[dict] = None
        self._stats_cache_at = 0.0
        self._tasks: set = set()

        if not uri:
            logger.critical("MONGO_URI is not set. User tracking is disabled.")
            return

        try:
            self._client = AsyncIOMotorClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                retryWrites=True,
                appname="video-bot",
            )
            db = self._client[db_name]
            self._col = db[USERS_COLLECTION]
            self._stats_col = db[STATS_COLLECTION]
        except (ConfigurationError, PyMongoError, ValueError):
            # Never log the URI: it contains credentials.
            logger.critical("Invalid MongoDB configuration. User tracking disabled.")
            self._client = None
            self._col = None
            self._stats_col = None

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @property
    def is_ready(self) -> bool:
        return self._col is not None

    @staticmethod
    def is_owner(user_id: Any) -> bool:
        return _valid_id(user_id) and user_id in OWNER_IDS

    async def _ensure_indexes(self) -> None:
        if self._indexed or not self.is_ready:
            return
        try:
            await self._col.create_index("username", sparse=True)
            await self._col.create_index("banned", sparse=True)
            await self._col.create_index("last_seen")
            await self._col.create_index("joined_at")
            self._indexed = True
        except PyMongoError:
            logger.exception("Could not create MongoDB indexes.")

    # ------------------------------------------------------------------ #
    # Users
    # ------------------------------------------------------------------ #
    async def _upsert(self, user_id, first_name, last_name, username) -> Optional[bool]:
        """True = brand new user, False = existing user, None = error."""
        if not self.is_ready or not _valid_id(user_id):
            return None
        await self._ensure_indexes()
        now = datetime.now(timezone.utc)
        try:
            result = await self._col.update_one(
                {"_id": user_id},
                {
                    "$set": {
                        "first_name": _clean(first_name, _MAX_NAME_LEN),
                        "last_name": _clean(last_name, _MAX_NAME_LEN),
                        "username": _clean(username, _MAX_USERNAME_LEN),
                        "last_seen": now,
                    },
                    "$setOnInsert": {"joined_at": now, "banned": False},
                },
                upsert=True,
            )
            return result.upserted_id is not None
        except DuplicateKeyError:
            return False        # another coroutine inserted the same user first
        except PyMongoError:
            logger.exception("MongoDB error while saving user %s", user_id)
            return None
        except Exception:
            logger.exception("Unexpected error while saving user %s", user_id)
            return None

    async def add_user(self, user_id, first_name, last_name, username) -> bool:
        """True only if this call created a new user."""
        return bool(await self._upsert(user_id, first_name, last_name, username))

    async def get_user(self, user_id: int) -> Optional[dict]:
        if not self.is_ready:
            return None
        try:
            return await self._col.find_one({"_id": user_id})
        except PyMongoError:
            logger.exception("MongoDB error while fetching user %s", user_id)
            return None

    async def is_user_exist(self, user_id: int) -> bool:
        if not self.is_ready:
            return False
        try:
            return await self._col.count_documents({"_id": user_id}, limit=1) > 0
        except PyMongoError:
            logger.exception("MongoDB error while checking user %s", user_id)
            return False

    async def total_users(self) -> int:
        if not self.is_ready:
            return 0
        try:
            return await self._col.count_documents({})
        except PyMongoError:
            logger.exception("MongoDB error while counting users")
            return 0

    async def delete_user(self, user_id: int) -> bool:
        if not self.is_ready:
            return False
        try:
            result = await self._col.delete_one({"_id": user_id})
            self._banned.discard(user_id)
            return result.deleted_count > 0
        except PyMongoError:
            logger.exception("MongoDB error while deleting user %s", user_id)
            return False

    # ------------------------------------------------------------------ #
    # Ban system
    # ------------------------------------------------------------------ #
    async def load_banned(self) -> bool:
        """Load every banned id into memory. Safe to call many times."""
        if not self.is_ready:
            return False
        if self._ban_lock is None:
            self._ban_lock = asyncio.Lock()
        async with self._ban_lock:
            if self._ban_loaded:
                return True
            try:
                ids = set()
                async for doc in self._col.find({"banned": True}, {"_id": 1}):
                    ids.add(doc["_id"])
                self._banned = ids
                self._ban_loaded = True
                return True
            except PyMongoError:
                logger.exception("Could not load the banned-user list.")
                return False

    async def is_banned(self, user_id: int) -> bool:
        """Fast, cached check. Owners can never be banned."""
        if not _valid_id(user_id) or user_id in OWNER_IDS:
            return False
        if not self._ban_loaded:
            await self.load_banned()
        return user_id in self._banned

    async def ban_user(self, user_id: int, by: int) -> str:
        """Returns: ok | already | not_found | owner | error"""
        if not _valid_id(user_id):
            return "error"
        if user_id in OWNER_IDS:
            return "owner"
        if not self.is_ready:
            return "error"
        try:
            doc = await self._col.find_one({"_id": user_id}, {"banned": 1})
            if doc is None:
                return "not_found"
            if doc.get("banned") is True:
                self._banned.add(user_id)
                return "already"
            result = await self._col.update_one(
                {"_id": user_id, "banned": {"$ne": True}},
                {"$set": {
                    "banned": True,
                    "banned_at": datetime.now(timezone.utc),
                    "banned_by": by,
                }},
            )
            if result.modified_count:
                self._banned.add(user_id)
                return "ok"
            return "already"
        except PyMongoError:
            logger.exception("MongoDB error while banning %s", user_id)
            return "error"

    async def unban_user(self, user_id: int) -> str:
        """Returns: ok | not_banned | not_found | error"""
        if not _valid_id(user_id) or not self.is_ready:
            return "error"
        try:
            doc = await self._col.find_one({"_id": user_id}, {"banned": 1})
            if doc is None:
                return "not_found"
            if doc.get("banned") is not True:
                self._banned.discard(user_id)
                return "not_banned"
            result = await self._col.update_one(
                {"_id": user_id, "banned": True},
                {
                    "$set": {"banned": False},
                    "$unset": {"banned_at": "", "banned_by": ""},
                },
            )
            self._banned.discard(user_id)
            return "ok" if result.modified_count else "not_banned"
        except PyMongoError:
            logger.exception("MongoDB error while unbanning %s", user_id)
            return "error"

    # ------------------------------------------------------------------ #
    # Task statistics (called from choice.py)
    # ------------------------------------------------------------------ #
    async def record_task(self, title: str, outcome: str) -> None:
        """Count one finished / failed / cancelled task. Never raises."""
        if self._stats_col is None or outcome not in TASK_OUTCOMES:
            return
        kind = TASK_KINDS.get(title, "other")
        try:
            await self._stats_col.update_one(
                {"_id": "tasks"},
                {"$inc": {outcome: 1, f"kinds.{kind}.{outcome}": 1}},
                upsert=True,
            )
        except PyMongoError:
            logger.exception("Could not record task statistics.")
        except Exception:
            logger.exception("Unexpected error while recording a task.")

    async def get_stats(self) -> dict:
        """Aggregated numbers for /stats (cached for a few seconds)."""
        now = time.monotonic()
        if self._stats_cache and now - self._stats_cache_at < _STATS_CACHE_TTL:
            return dict(self._stats_cache)

        empty = {
            "total": 0, "banned": 0, "active_24h": 0, "new_24h": 0,
            "finished": 0, "failed": 0, "cancelled": 0, "kinds": {},
            "ok": False,
        }
        if not self.is_ready:
            return empty

        since = datetime.now(timezone.utc) - timedelta(hours=24)
        try:
            total, banned, active, new, tasks = await asyncio.gather(
                self._col.count_documents({}),
                self._col.count_documents({"banned": True}),
                self._col.count_documents({"last_seen": {"$gte": since}}),
                self._col.count_documents({"joined_at": {"$gte": since}}),
                self._stats_col.find_one({"_id": "tasks"}),
            )
        except PyMongoError:
            logger.exception("MongoDB error while building stats.")
            return dict(self._stats_cache) if self._stats_cache else empty

        tasks = tasks or {}
        result = {
            "total": total,
            "banned": banned,
            "active_24h": active,
            "new_24h": new,
            "finished": int(tasks.get("finished", 0)),
            "failed": int(tasks.get("failed", 0)),
            "cancelled": int(tasks.get("cancelled", 0)),
            "kinds": tasks.get("kinds", {}) if isinstance(tasks.get("kinds"), dict) else {},
            "ok": True,
        }
        self._stats_cache = result
        self._stats_cache_at = now
        return dict(result)

    # ------------------------------------------------------------------ #
    # Broadcast support
    # ------------------------------------------------------------------ #
    async def count_broadcast_users(self) -> int:
        """Users who will receive a broadcast (everyone not banned)."""
        if not self.is_ready:
            return 0
        try:
            return await self._col.count_documents({"banned": {"$ne": True}})
        except PyMongoError:
            logger.exception("MongoDB error while counting broadcast users")
            return 0

    async def iter_user_ids(self) -> AsyncIterator[int]:
        """Stream user ids (not banned) without loading them all in memory."""
        if not self.is_ready:
            return
        cursor = self._col.find({"banned": {"$ne": True}}, {"_id": 1}).batch_size(1000)
        async for doc in cursor:
            yield doc["_id"]

    # ------------------------------------------------------------------ #
    # Main entry point for handlers
    # ------------------------------------------------------------------ #
    async def register_user(self, client, user) -> bool:
        """
        Save/refresh a Telegram user; log new users to LOG_CHANNEL.
        Never raises. Returns True if the user was new.
        Writes are throttled (once per TOUCH_INTERVAL per user) so calling
        this on every message is cheap.
        """
        try:
            if user is None or getattr(user, "is_bot", False):
                return False
            user_id = getattr(user, "id", None)
            if not _valid_id(user_id):
                return False

            now = time.monotonic()
            last = self._touched.get(user_id)
            if last is not None and now - last < TOUCH_INTERVAL:
                return False

            is_new = await self._upsert(
                user_id,
                getattr(user, "first_name", None),
                getattr(user, "last_name", None),
                getattr(user, "username", None),
            )
            if is_new is None:
                return False

            if len(self._touched) >= _MAX_TOUCH_CACHE:
                self._touched.clear()
            self._touched[user_id] = now

            if is_new:
                # Background task: a slow log channel never delays the user.
                task = asyncio.create_task(self._notify_new_user(client, user))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            return is_new
        except Exception:
            logger.exception("register_user failed")
            return False

    async def _notify_new_user(self, client, user) -> None:
        """Send the 'new user' message to LOG_CHANNEL. Never raises."""
        if self._log_channel is None:
            logger.warning("LOG_CHANNEL not configured; skipping new-user log.")
            return

        first = html.escape(getattr(user, "first_name", None) or "N/A")
        last = html.escape(getattr(user, "last_name", None) or "N/A")
        uname = getattr(user, "username", None)
        uname_text = f"@{html.escape(uname)}" if uname else "N/A"
        uid = int(user.id)

        text = (
            "<b>#NewUser</b>\n\n"
            f"<b>ID:</b> <code>{uid}</code>\n"
            f"<b>First Name:</b> {first}\n"
            f"<b>Last Name:</b> {last}\n"
            f"<b>Username:</b> {uname_text}\n"
            f'<b>Profile:</b> <a href="tg://user?id={uid}">Open</a>'
        )

        for attempt in range(2):
            try:
                await client.send_message(chat_id=self._log_channel, text=text)
                return
            except Exception as exc:
                wait = getattr(exc, "value", None)
                if (
                    attempt == 0
                    and exc.__class__.__name__ == "FloodWait"
                    and isinstance(wait, int)
                ):
                    logger.warning("FloodWait: retrying log message in %ss", wait)
                    await asyncio.sleep(min(wait, 60))
                    continue
                logger.exception(
                    "Could not send new-user log to channel %s "
                    "(is the bot an admin there?)", self._log_channel
                )
                return


# Single shared instance used across the whole bot.
user_db = UserDatabase(MONGO_URI, DB_NAME, LOG_CHANNEL)

