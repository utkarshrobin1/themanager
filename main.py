#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║            The Manager — Telegram Bot v2.0              ║
║       Full-featured Group Management Bot                ║
║       Rose Bot feature-parity + all fixes               ║
╠══════════════════════════════════════════════════════════╣
║  STORAGE ARCHITECTURE                                   ║
║  PRIMARY  : MongoDB  — persistent, survives redeploys   ║
║  SECONDARY: SQLite   — in-process cache, rebuilt auto   ║
║                                                          ║
║  Lookup order: Telegram API → MongoDB → SQLite          ║
║  Write  order: MongoDB first, then SQLite               ║
╠══════════════════════════════════════════════════════════╣
║  Deploy on Railway/Render/Replit safely:                ║
║  Set env var  BOT_TOKEN=...  and optionally             ║
║  MONGO_URI=mongodb+srv://...  (defaults to hardcoded)   ║
╚══════════════════════════════════════════════════════════╝

Dependencies:
    pip install python-telegram-bot==20.7 pymongo dnspython

Run:
    python themanager.py
"""

import asyncio
import random
import sqlite3
import logging
import re
import time
import json
import os
import html
import uuid
import base64
import hashlib
import string as _string
from datetime import datetime, timedelta
from functools import wraps

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

from telegram import (
    Update,
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.error import BadRequest, Forbidden, TelegramError

# ══════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID  = 6336459877
MONGO_URI = "mongodb+srv://eclbot:eclbot1234@cluster0.eamckjk.mongodb.net/?appName=Cluster0"

# ── Action image (optional) ────────────────────────────────────────────────
# Set a Telegram file_id (photo) here to send ban/kick/mute as image caption.
# Leave empty "" to send plain text only.
ACTION_PHOTO_FILE_ID = ""   # e.g. "AgACAgIAAxkBAAI..."
# ──────────────────────────────────────────────────────────────────────────

# Commands older than this many seconds are silently ignored (anti-overload).
CMD_MAX_AGE_SECS = 180   # 3 minutes

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
#  MONGODB
# ══════════════════════════════════════════════════════════
mongo_users_col = None
mongo_gbans_col = None
mongo_stats_col = None
mongo_groups_col = None


def init_mongo():
    global mongo_users_col, mongo_gbans_col, mongo_stats_col, mongo_groups_col
    if not MONGO_AVAILABLE:
        logger.warning("pymongo not installed. pip install pymongo dnspython")
        return
    if not MONGO_URI:
        logger.warning("MONGO_URI not set — MongoDB disabled. User data will NOT persist across redeploys!")
        return
    try:
        import dns.resolver
        dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
        dns.resolver.default_resolver.nameservers = ["8.8.8.8", "8.8.4.4", "1.1.1.1"]
    except Exception:
        pass
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=8000,
            socketTimeoutMS=10000,
            maxPoolSize=10,
        )
        client.server_info()
        db = client["themanager"]
        mongo_users_col = db["users"]
        mongo_gbans_col = db["gbans"]
        mongo_stats_col = db["bot_stats"]
        mongo_groups_col = db["bot_groups"]

        # bot_stats index
        try:
            mongo_stats_col.create_index("key", unique=True)
        except Exception:
            pass
        # bot_groups index
        try:
            mongo_groups_col.create_index("chat_id", unique=True)
        except Exception:
            pass

        # users indexes
        for idx_kwargs in [
            {"keys": "user_id",          "unique": True,  "sparse": False},
            {"keys": "username",          "sparse": True},
            {"keys": "username_history",  "sparse": True},
            {"keys": "seen_in_chats",     "sparse": True},
            {"keys": "id",                "sparse": True},   # legacy compat
        ]:
            try:
                mongo_users_col.create_index(**idx_kwargs)
            except Exception:
                pass

        # gbans indexes
        try:
            mongo_gbans_col.create_index("user_id", unique=True, sparse=False)
        except Exception:
            pass

        user_count = mongo_users_col.estimated_document_count()
        gban_count = mongo_gbans_col.estimated_document_count()
        logger.info("MongoDB PRIMARY STORAGE — ACTIVE")
        logger.info(f"  Cached users : {user_count}")
        logger.info(f"  Global bans  : {gban_count}")
        logger.info("  User data persists across ALL redeploys")
    except Exception as e:
        logger.warning(f"MongoDB connection failed: {e}")
        logger.warning("Falling back to SQLite-only mode. User data will be lost on redeploy!")
        mongo_users_col = None


def _mongo_uid(doc) -> int | None:
    """Extract user_id from a mongo doc that may use 'user_id' or 'id'."""
    return doc.get("user_id") or doc.get("id")


def _mongo_fname(doc) -> str:
    """Extract first_name from a mongo doc that may use different keys."""
    return doc.get("first_name") or doc.get("name") or doc.get("fname") or "Unknown"


def _mongo_uname(doc) -> str | None:
    return doc.get("username") or doc.get("user_name")


def mongo_upsert_user(user, chat_id: int = None) -> None:
    """PRIMARY user upsert — always write to MongoDB first.

    Schema per document:
      user_id        int  — canonical Telegram user ID (unique index)
      id             int  — legacy alias for user_id (kept for old docs)
      first_name     str
      last_name      str | None
      username       str | None  — lowercase, current username
      username_history [str]     — all past usernames (for stale @mention lookups)
      last_seen      int  — unix timestamp
      seen_in_chats  [int] — all group chat_ids this user was seen in
    """
    if mongo_users_col is None:
        return
    try:
        uname = getattr(user, "username", None)
        uname_lower = uname.lower() if uname else None
        now = int(time.time())

        doc_set = {
            "user_id":    user.id,
            "id":         user.id,         # legacy field — keep in sync
            "first_name": user.first_name or "Unknown",
            "last_name":  getattr(user, "last_name", None),
            "username":   uname_lower,
            "last_seen":  now,
        }
        update_op: dict = {"$set": doc_set}

        # Track chat membership
        if chat_id:
            update_op["$addToSet"] = {"seen_in_chats": chat_id}  # type: ignore[assignment]

        # Track username history so old @mentions still resolve after a rename
        if uname_lower:
            add_to_set = update_op.get("$addToSet", {})
            add_to_set["username_history"] = uname_lower
            update_op["$addToSet"] = add_to_set

        mongo_users_col.update_one({"user_id": user.id}, update_op, upsert=True)
    except Exception:
        pass


def mongo_upsert_group_member(chat_id: int, user) -> None:
    """Write per-group member record into MongoDB with group context.

    This is the PRIMARY persistent storage call for group membership.
    Stores chat_id in seen_in_chats array and updates username_history.
    """
    if mongo_users_col is None:
        return
    mongo_upsert_user(user, chat_id=chat_id)


def mongo_find_by_username(username: str):
    """Find user by current username OR any historical username.

    Search priority:
      1. Current username field (exact, indexed)
      2. username_history array (indexed) — handles renamed users
      3. Legacy user_name field (old documents)
    """
    if mongo_users_col is None:
        return None
    clean = username.lower().lstrip("@")
    try:
        # Single $or query — one round-trip instead of three
        doc = mongo_users_col.find_one({
            "$or": [
                {"username": clean},
                {"username_history": clean},
                {"user_name": clean},   # legacy field
            ]
        })
        return doc
    except Exception:
        return None


def mongo_find_by_id(user_id: int):
    """Find user by Telegram user_id. Checks both user_id and legacy id field."""
    if mongo_users_col is None:
        return None
    try:
        # user_id has a unique index — fast O(log n) lookup
        doc = mongo_users_col.find_one({"user_id": user_id})
        if doc:
            return doc
        # Legacy documents may only have "id"
        return mongo_users_col.find_one({"id": user_id})
    except Exception:
        return None


def is_gbanned(user_id: int):
    if mongo_gbans_col is None:
        return None
    try:
        doc = mongo_gbans_col.find_one({"user_id": user_id})
        if doc:
            return doc
        return mongo_gbans_col.find_one({"id": user_id})
    except Exception:
        return None


def gban_user(user_id: int, reason: str, by: int):
    if mongo_gbans_col is None:
        return
    try:
        mongo_gbans_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "id": user_id,
                      "reason": reason, "by": by, "time": int(time.time())}},
            upsert=True,
        )
    except Exception:
        pass


def ungban_user(user_id: int) -> bool:
    if mongo_gbans_col is None:
        return False
    try:
        r = mongo_gbans_col.delete_one({"user_id": user_id})
        if r.deleted_count:
            return True
        r = mongo_gbans_col.delete_one({"id": user_id})
        return bool(r.deleted_count)
    except Exception:
        return False

# ══════════════════════════════════════════════════════════
#  SQLITE DATABASE
# ══════════════════════════════════════════════════════════
conn   = sqlite3.connect("themanager.db", check_same_thread=False)
cursor = conn.cursor()


def init_db():
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS notes (
            chat_id INTEGER, name TEXT, content TEXT, is_private INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, name)
        );
        CREATE TABLE IF NOT EXISTS filters (
            chat_id INTEGER, keyword TEXT, response TEXT,
            PRIMARY KEY (chat_id, keyword)
        );
        CREATE TABLE IF NOT EXISTS warns (
            chat_id INTEGER, user_id INTEGER,
            count INTEGER DEFAULT 0, reasons TEXT DEFAULT '',
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS rules (
            chat_id INTEGER PRIMARY KEY, rules_text TEXT, private_rules INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS welcome (
            chat_id INTEGER PRIMARY KEY,
            welcome_text TEXT, goodbye_text TEXT,
            welcome_enabled INTEGER DEFAULT 1,
            goodbye_enabled INTEGER DEFAULT 1,
            clean_welcome INTEGER DEFAULT 0,
            clean_service INTEGER DEFAULT 0,
            last_welcome_id INTEGER DEFAULT 0,
            welcome_buttons TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS locks (
            chat_id INTEGER PRIMARY KEY,
            sticker INTEGER DEFAULT 0, link INTEGER DEFAULT 0,
            forward INTEGER DEFAULT 0, photo INTEGER DEFAULT 0,
            video INTEGER DEFAULT 0, document INTEGER DEFAULT 0,
            audio INTEGER DEFAULT 0, voice INTEGER DEFAULT 0,
            gif INTEGER DEFAULT 0, poll INTEGER DEFAULT 0,
            contact INTEGER DEFAULT 0, location INTEGER DEFAULT 0,
            game INTEGER DEFAULT 0, inline INTEGER DEFAULT 0,
            rtl INTEGER DEFAULT 0, button INTEGER DEFAULT 0,
            all_media INTEGER DEFAULT 0, text INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS adminlocks (
            chat_id INTEGER PRIMARY KEY,
            sticker INTEGER DEFAULT 0, gif INTEGER DEFAULT 0,
            text INTEGER DEFAULT 0, url INTEGER DEFAULT 0,
            photo INTEGER DEFAULT 0, video INTEGER DEFAULT 0,
            document INTEGER DEFAULT 0, audio INTEGER DEFAULT 0,
            voice INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS flood (
            chat_id INTEGER PRIMARY KEY,
            limit_count INTEGER DEFAULT 0,
            action TEXT DEFAULT 'mute',
            time_window INTEGER DEFAULT 5,
            tmute_duration INTEGER DEFAULT 300
        );
        CREATE TABLE IF NOT EXISTS blacklist (
            chat_id INTEGER, word TEXT, action TEXT DEFAULT 'del',
            PRIMARY KEY (chat_id, word)
        );
        CREATE TABLE IF NOT EXISTS afk (
            user_id INTEGER PRIMARY KEY,
            reason TEXT, set_time INTEGER
        );
        CREATE TABLE IF NOT EXISTS settings (
            chat_id INTEGER PRIMARY KEY,
            max_warns INTEGER DEFAULT 3,
            warn_action TEXT DEFAULT 'ban',
            strong_warn INTEGER DEFAULT 0,
            antibot INTEGER DEFAULT 0,
            antispam INTEGER DEFAULT 0,
            clean_linked INTEGER DEFAULT 0,
            log_channel INTEGER DEFAULT 0,
            forcesub_channel TEXT DEFAULT '',
            locale TEXT DEFAULT 'en'
        );
        CREATE TABLE IF NOT EXISTS user_cache (
            -- SECONDARY in-process cache. MongoDB is authoritative and persistent.
            -- Data here is rebuilt automatically as users send messages.
            user_id INTEGER PRIMARY KEY,
            first_name TEXT, last_name TEXT,
            username TEXT, last_seen INTEGER
        );
        CREATE TABLE IF NOT EXISTS approved (
            chat_id INTEGER, user_id INTEGER,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS disabled_cmds (
            chat_id INTEGER, command TEXT,
            PRIMARY KEY (chat_id, command)
        );
        CREATE TABLE IF NOT EXISTS connections (
            user_id INTEGER PRIMARY KEY, chat_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS feds (
            fed_id TEXT PRIMARY KEY, fed_name TEXT,
            owner_id INTEGER, admins TEXT DEFAULT '[]',
            chats TEXT DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS fedbans (
            fed_id TEXT, user_id INTEGER, reason TEXT,
            PRIMARY KEY (fed_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS whitelist (
            chat_id INTEGER, user_id INTEGER,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS bot_stats (
            key TEXT PRIMARY KEY, value INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bot_groups (
            chat_id INTEGER PRIMARY KEY,
            title TEXT, username TEXT, joined_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS dm_users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT, username TEXT, started_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS chat_members (
            -- SECONDARY cache only. MongoDB (seen_in_chats array) is authoritative.
            -- This table is rebuilt from messages after a redeploy automatically.
            chat_id INTEGER,
            user_id INTEGER,
            first_name TEXT,
            username TEXT,
            last_seen INTEGER,
            PRIMARY KEY (chat_id, user_id)
        );
    """)
    conn.commit()

init_db()

# ── Schema migrations (safe to run on every start) ────────
def _migrate_db():
    """Add any missing columns to existing tables without dropping data."""
    migrations = [
        ("locks", "text", "INTEGER DEFAULT 0"),
        ("filters", "match_type", "TEXT DEFAULT 'word'"),
    ]
    for table, col, col_def in migrations:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
            conn.commit()
            logger.info(f"Migration: added column '{col}' to '{table}'")
        except Exception:
            pass  # Column already exists

_migrate_db()

# ══════════════════════════════════════════════════════════
#  @all  IN-PROGRESS TRACKER
# ══════════════════════════════════════════════════════════
# { chat_id: {"task": asyncio.Task, "cancelled": bool} }
_tag_all_tasks: dict = {}

# ══════════════════════════════════════════════════════════
#  TAG EMOJIS  —  used by @all feature
# ══════════════════════════════════════════════════════════
TAG_EMOJIS = [
    "🎀","👀","🌟","🔥","⚡","💫","🎯","🎪","🎭","🎨",
    "🌈","🦋","🌺","🎵","🎶","💎","🏆","🎲","🎸","🌙",
    "☀️","🌊","🍀","🎀","💡","🎃","🎄","🌸","🦄","🌴",
    "🎠","🎡","🎢","🎪","🎰","🎳","🏅","🥇","🎖️","🏵️",
]

# ══════════════════════════════════════════════════════════
#  ROAST / REPLY POOLS  — HTML bold, no markdown stars
# ══════════════════════════════════════════════════════════

WELCOME_MSGS = [
    "🎉 Hey {mention}! Welcome to <b>{chat}</b>! Great to have you here!",
    "👋 Welcome aboard, {mention}! Hope you enjoy <b>{chat}</b>!",
    "🌟 A new star has arrived! Welcome, {mention}, to <b>{chat}</b>!",
    "🚀 {mention} just landed in <b>{chat}</b>! Fasten your seatbelt 🚀",
    "🎊 We've been waiting for you, {mention}! Welcome!",
    "💫 {mention} has entered the chat! <b>{chat}</b> just levelled up!",
    "🔥 Look who showed up! Hey {mention}, welcome to <b>{chat}</b>!",
    "🎭 The legend {mention} has arrived at <b>{chat}</b>!",
    "🌈 {mention} just joined. The energy in <b>{chat}</b> shifted. Noticeably.",
    "⚡ Alert! A new member detected: {mention}! Welcome to <b>{chat}</b>!",
    "🎯 Perfect timing, {mention}! <b>{chat}</b> needed exactly you!",
    "👑 Royalty has arrived — welcome, {mention}, to <b>{chat}</b>!",
    "🌺 {mention} bloomed into <b>{chat}</b>! We're glad you're here!",
    "🎵 drumroll — Introducing {mention} to <b>{chat}</b>! Welcome!",
]

GOODBYE_MSGS = [
    "😢 {mention} has left <b>{chat}</b>. We'll miss you!",
    "👋 Goodbye, {mention}! Hope to see you again soon!",
    "💔 {mention} just left. The group won't be the same.",
    "🌙 Until next time, {mention}! Take care out there.",
    "😔 {mention} has left the building. Bye bye! 👋",
    "🍃 {mention} drifted away from <b>{chat}</b>. May the winds guide you back.",
    "🌊 {mention} sailed out of <b>{chat}</b>. Safe travels!",
    "🕯️ A candle dims as {mention} leaves <b>{chat}</b>. The warmth will be missed.",
]

BAN_MSGS = [
    "🔨 <b>{user}</b> has been banned! Don't trip on the ban hammer on your way out, sweetheart.",
    "🚫 <b>{user}</b> just got permanently evicted. Even the group's trash bin rejected you.",
    "⚡ <b>{user}</b> has been yeeted into the shadow realm. The group IQ immediately rose 40 points.",
    "🛡️ <b>{user}</b> — banned. Scientists are studying how one person can be this consistently wrong.",
    "💀 <b>{user}</b> is gone. Your ancestors are embarrassed. Your descendants will change their last name.",
    "🧹 Swept <b>{user}</b> out like the dirt they are. The mop gagged a little.",
    "🎺 Plays world's smallest violin — <b>{user}</b> has been exiled. No one is crying. Not even a little.",
    "📦 <b>{user}</b> has been boxed up, taped shut, and shipped to nowhere. Return address: irrelevance.",
    "🚀 <b>{user}</b> launched straight into the ban dimension. Even the void doesn't want them back.",
    "🪦 RIP <b>{user}</b>. Cause of death: terminal stupidity. The group sends zero condolences.",
    "🔥 <b>{user}</b> got burned by the ban hammer. The ashes have been swept up and thrown into the wind.",
    "😤 <b>{user}</b> has been removed. The group's collective sigh of relief registered on the Richter scale.",
    "🧂 <b>{user}</b> — banned. Salt of the earth, you were. Pure, raw, uncut sodium. Goodbye.",
    "🎪 <b>{user}</b> is gone! The circus lost its main act, but gained measurably better air quality.",
    "💅 Bestie, <b>{user}</b> got banned. The audacity, the nerve, the un-be-liev-able gall. Deleted.",
    "⚰️ <b>{user}</b> has been buried. The headstone reads: 'They really tried it. They really did.'",
    "🧟 <b>{user}</b> — banned and spiritually exorcised. This group is now 100% {user}-free.",
    "🎯 Clean shot. <b>{user}</b> banned. No hesitation, no regret, no forwarding address.",
    "🌋 <b>{user}</b> erupted for the last time. Banned. The group is now cooling down nicely.",
    "🤡 The clown has been removed from the circus. <b>{user}</b> is banned. Other clowns, take notes.",
    "🦗 <b>{user}</b> has been exterminated. The pest control bill was worth every penny.",
    "🗑️ <b>{user}</b> — compressed, zipped, encrypted, and permanently deleted. No recycle bin.",
    "📵 <b>{user}</b> banned. Even the group's most forgiving member said 'yeah, fair enough.'",
    "🏚️ <b>{user}</b> has been evicted. The landlord would like to remind remaining members: read the rules.",
    "💣 <b>{user}</b> detonated and was promptly banned. The blast radius was contained. We're fine.",
    "🧊 <b>{user}</b> — frozen out. Not banned because we're angry. Banned because we're done.",
    "🌪️ <b>{user}</b> swept themselves right out of this group. Nature and moderators work in harmony.",
    "🎭 The curtain falls on <b>{user}</b>. Their performance was exhausting and nobody asked for an encore.",
    "📏 <b>{user}</b> has been measured and found wanting. Banned by 0.00% popular demand.",
    "🐛 <b>{user}</b> banned. Evolution has many paths. This was not one of the successful ones.",
]

KICK_MSGS = [
    "👢 <b>{user}</b> has been kicked! Come back when evolution finishes what it started.",
    "🦵 <b>{user}</b> got kicked! The door is that way 👉 — try not to mistake it for a wall again.",
    "⚡ <b>{user}</b> removed at supersonic speed. Even the exit sign is judging you.",
    "🚪 <b>{user}</b> was kicked! The group collectively exhaled for the first time in weeks.",
    "🎯 <b>{user}</b> kicked clean out. Accuracy: 100%. Regret: 0%.",
    "😤 <b>{user}</b> has been physically removed. Thank you for flying Air Kick, the airline for people nobody invited.",
    "🏌️ FORE! <b>{user}</b> has been golfed out of the group. Shame drives further than talent, apparently.",
    "🗑️ <b>{user}</b> — taken out like the trash. Scheduled for once a week but needed today.",
    "🦶 <b>{user}</b> got the boot! Italian leather, size twelve, applied at maximum velocity.",
    "⛳ <b>{user}</b> sent flying! Hole-in-one for the admins. Par for the course.",
    "🎳 Strike! <b>{user}</b> knocked clean out of the group. Clean-up crew standing by.",
    "🏈 <b>{user}</b> punted out of the group! The admins just went pro.",
    "🚂 <b>{user}</b> missed the group's train. It departed on schedule without them. As planned.",
    "🎪 The ringmaster has ejected <b>{user}</b> from the tent. The remaining circus continues.",
    "📤 <b>{user}</b> has been sent. Recipient: nowhere. Delivery status: permanent.",
    "🌊 <b>{user}</b> wiped out! Surfed straight off the edge. No rescue boat dispatched.",
    "🧲 <b>{user}</b> repelled by the group's natural antibodies. Immune response successful.",
    "🪃 <b>{user}</b> kicked — and unlike a boomerang, we don't want this one coming back.",
    "🛸 <b>{user}</b> abducted by the kick beam. Scientists say this particular specimen needed further study. Elsewhere.",
    "🎲 <b>{user}</b> rolled a 1 on the 'staying in the group' check. Critical fail. Kicked.",
]

MUTE_MSGS = [
    "🔇 <b>{user}</b> has been muted! Silence is golden. You, however, are not even copper.",
    "🤐 <b>{user}</b> can't talk anymore! The group's average intelligence just increased measurably.",
    "📵 <b>{user}</b> is now silenced. Birds are singing. Children are laughing. The world healed.",
    "🔕 <b>{user}</b> muted! Your words were adding negative value. This is an upgrade.",
    "🎙️ <b>{user}</b>'s mic has been confiscated, incinerated, and the ashes scattered at sea.",
    "🧲 <b>{user}</b>'s mouth has been magnetically sealed. The science of mercy has never been more beautiful.",
    "🫡 <b>{user}</b> has been silenced. Everyone is pretending to be sad. Nobody is sad.",
    "🦜 Welp, <b>{user}</b> has been muted. Even parrots have standards about what they'll repeat.",
    "🔈 Volume at zero. <b>{user}</b> muted. The group's noise pollution dropped to acceptable levels.",
    "📻 <b>{user}</b>'s broadcast has been cut. Programming note: the silence is intentional and welcome.",
    "🧏 <b>{user}</b> is now speaking in sign language. Unfortunately, nobody here learned sign language for them.",
    "🪣 <b>{user}</b>'s words have been bucket-caught and disposed of safely. Muted.",
    "📺 <b>{user}</b> — channel changed. Nobody was watching anyway, but now it's officially off.",
    "🎭 <b>{user}</b> has been muted. The stage is cleared. The audience breathes.",
    "🌿 <b>{user}</b> silenced. Nature is healing. The birds returned. The rivers run clear.",
    "💊 <b>{user}</b> prescribed silence by Dr. Admin. Dosage: indefinite. Refills: none.",
    "🗣️ <b>{user}</b> muted. The transformation was swift and universally applauded.",
    "🔧 <b>{user}</b>'s vocal cords technically fixed. By removing their ability to use them here.",
    "🧸 <b>{user}</b> reduced to the communication level of a stuffed animal. This is an improvement.",
    "⛔ <b>{user}</b>'s message privileges revoked. The committee voted unanimously. There was applause.",
]

UNMUTE_MSGS = [
    "🔊 <b>{user}</b> can speak again! Welcome back to the noise. Please don't ruin it.",
    "🗣️ <b>{user}</b> is unmuted! Behave this time. We're watching. 👀",
    "✅ <b>{user}</b> is now unmuted. Don't make us regret this decision.",
    "🎤 Mic restored to <b>{user}</b>. Use it wisely. Or don't, and get muted again, your call.",
    "🔓 <b>{user}</b> is free to speak! The silence was nice while it lasted though.",
    "🕊️ Peace offering accepted. <b>{user}</b> is unmuted. The group watches with cautious optimism.",
    "📣 <b>{user}</b> is back online! This is either great news or a countdown to the next mute.",
    "🫢 <b>{user}</b> has been unsilenced. The group collectively braces itself.",
]

WARN_MSGS = [
    "⚠️ <b>{user}</b> has been warned! [{count}/{max}] — tick tock, champ. That clock isn't decorative.",
    "🚨 Warning issued to <b>{user}</b>! [{count}/{max}] — actively speedrunning a ban. Any% apparently.",
    "⚡ <b>{user}</b>, that's #{count} of {max}. Impressive dedication to being absolutely insufferable.",
    "🎲 <b>{user}</b> rolled the dice and landed on stupid. [{count}/{max}] — bold recurring strategy.",
    "📋 <b>{user}</b> has collected warn #{count}/{max}. At this rate you'll have a full set.",
    "😬 <b>{user}</b> is genuinely trying to get banned. [{count}/{max}] — we see you. Unfortunately.",
    "🏆 <b>{user}</b> is {count}/{max} warns deep. This isn't an achievement. Stop treating it like one.",
    "🔢 <b>{user}</b> — warn #{count} of {max}. Your future here has the structural integrity of wet tissue.",
    "🎯 <b>{user}</b> collected warn #{count}/{max}. Consistency is a virtue. This is not.",
    "🎭 <b>{user}</b> earns warn {count}/{max}. The admins are genuinely impressed by the commitment to chaos.",
    "🔔 Ding — warn #{count}/{max} for <b>{user}</b>. Pavlov's dog would have learned by now.",
    "📊 <b>{user}</b>: {count}/{max} warns. The trajectory is not looking good. Statistically.",
    "🧮 Warn {count} of {max} issued to <b>{user}</b>. The math says you have {remaining} left. Do the math.",
    "🃏 <b>{user}</b> drew warn #{count}. The deck only has {max} cards. You've played {count}. Think about it.",
    "⏳ <b>{user}</b> — warn {count}/{max}. The hourglass is nearly empty. One guess what's at the bottom.",
    "🌡️ <b>{user}</b> temperature rising! Warn {count}/{max}. Critical threshold approaching.",
    "🎰 <b>{user}</b> pulls warn #{count}/{max} on the slot machine of consequences.",
    "🧱 <b>{user}</b> adds brick #{count} to their wall of warnings. {remaining} more and the wall is complete.",
    "💣 <b>{user}</b> — fuse lit. Warn {count}/{max}. Someone's counting down.",
    "📉 <b>{user}</b>'s group tenure chart is trending down. Warn {count}/{max}. Analysts are concerned.",
]

NO_PERM_MSGS = [
    "🚫 You're not an admin, buddy! Sit down.",
    "❌ Admins only! Nice try though 👏",
    "⛔ This command is for admins only. Which you are not. Awkward.",
    "🔒 You don't have the power for this! The power chose someone else.",
    "😂 Nice try, chief. Admins only!",
    "🪑 Please take a seat. Admin chairs are reserved.",
    "🎭 Bold of you to assume you have permissions. Adorable, really.",
    "🛑 Halt! Admin checkpoint. No entry without a badge.",
    "💅 Only admins can do that, and sweetie — you are NOT one.",
    "🎪 Step right up, step right— oh wait, you're not an admin. Step back.",
    "🧑‍⚖️ The court rules: insufficient permissions. Case dismissed.",
    "🔑 Wrong key. Wrong door. Wrong person. Try again never.",
    "📜 Admin scroll checked. Your name? Not on it.",
    "🏰 The castle drawbridge remains raised. Admins only inside.",
    "👻 The admin powers phased right through you. Haunting.",
    "🦁 Bold move, tiny roar. Still not an admin though.",
    "🎯 Swing and a miss. Admin verification failed. Try being promoted first.",
    "🧊 Chilled response: no. You are not an admin.",
    "📵 Permission denied. Permanently.",
    "🤖 ERROR 403: UNAUTHORIZED. You are not an admin. Have a nice day.",
]

KICKME_ROASTS = [
    "🚪 {mention} kicked themselves! The group collectively pretended to look sad. Nobody looked sad.",
    "💀 {mention} has been self-destructed! Even the group's most patient member visibly relaxed.",
    "🦵 {mention} kicked themselves! In a world full of questionable decisions, this one ranks high.",
    "😂 {mention} yeets themselves out. This is the most competent thing they've done here all week.",
    "🎉 {mention} is gone! The group IQ went up. The vibe improved. Plants grew. Birds returned.",
    "👋 Bye {mention}! You chose the door. The door chose you back. Healthiest relationship you've had.",
    "🙄 {mention} kicked themselves. The floor beneath them breathed a visible sigh of relief.",
    "🧠 {mention} self-evicted. First good call since they joined. Growth is beautiful.",
    "🎪 {mention} threw themselves out of the circus. The ringmaster didn't even look up.",
    "📦 {mention} self-packaged and self-shipped. Destination: outside. No tracking number needed.",
    "🏅 {mention} earns the award for 'Most Proactive Departure'. The medal is a boot.",
    "🌤️ {mention} left. Somewhere, a cloud parted. Coincidence? The sun doesn't think so.",
    "⚡ {mention} rage-quit existence in this group. Bold. Unasked for. But bold.",
    "🎵 And I will always love you — just kidding, nobody will. Bye {mention}!",
    "🔬 Scientists are studying {mention}'s self-kick. So far all we know is: it was warranted.",
]

GBAN_MSGS = [
    "🌍 <b>{user}</b> has been globally banned! A menace no more, across every group at once.",
    "⚡ GLOBAL BAN issued to <b>{user}</b>! The entire Telegram group ecosystem breathed easier.",
    "🔨 <b>{user}</b> globally hammered! One swing, every server. Efficiency is a virtue.",
    "🌐 <b>{user}</b> added to the global ban list. Some people collect trophies. You collect bans.",
    "📡 Broadcasting a ban to all corners: <b>{user}</b> — done, globally, permanently, finally.",
]

UNGBAN_MSGS = [
    "✅ <b>{user}</b> has been removed from the global ban list. Clean slate. Don't waste it.",
    "🌍 <b>{user}</b> ungbanned. The world is cautiously optimistic. Don't prove us wrong.",
    "🕊️ Global ban lifted from <b>{user}</b>. The universe is giving you a second chance. One.",
]

# ══════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════
flood_tracker: dict = {}
_bot_start_time = time.time()

FULL_PERMS = ChatPermissions(
    can_send_messages=True,
    can_send_audios=True,
    can_send_documents=True,
    can_send_photos=True,
    can_send_videos=True,
    can_send_video_notes=True,
    can_send_voice_notes=True,
    can_send_polls=True,
    can_send_other_messages=True,
    can_add_web_page_previews=True,
    can_change_info=False,
    can_invite_users=True,
    can_pin_messages=False,
)


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int = None) -> bool:
    chat = update.effective_chat
    if user_id is None:
        user_id = update.effective_user.id
    if user_id == OWNER_ID:
        return True
    try:
        member = await chat.get_member(user_id)
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except (BadRequest, TelegramError):
        return False


async def is_group_owner(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int = None) -> bool:
    if user_id is None:
        user_id = update.effective_user.id
    if user_id == OWNER_ID:
        return True
    try:
        member = await update.effective_chat.get_member(user_id)
        return member.status == ChatMemberStatus.OWNER
    except (BadRequest, TelegramError):
        return False


async def is_approved(chat_id: int, user_id: int) -> bool:
    cursor.execute("SELECT 1 FROM approved WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    return cursor.fetchone() is not None


async def is_whitelisted(chat_id: int, user_id: int) -> bool:
    cursor.execute("SELECT 1 FROM whitelist WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    return cursor.fetchone() is not None


def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.effective_chat or not update.effective_user:
            return
        if update.effective_chat.type == "private":
            await update.message.reply_text("⚠️ This command only works in groups!")
            return
        if not await is_admin(update, context):
            await update.message.reply_text(random.choice(NO_PERM_MSGS))
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def owner_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.effective_user:
            return
        if not update.message:
            return
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("🔒 This command is for the bot owner only!")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def _is_stale(update: Update) -> bool:
    """Return True if the message is older than CMD_MAX_AGE_SECS (anti-overload)."""
    if not update.message or not update.message.date:
        return False
    age = time.time() - update.message.date.timestamp()
    return age > CMD_MAX_AGE_SECS


def stale_guard(func):
    """Decorator: silently ignore commands older than CMD_MAX_AGE_SECS."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if _is_stale(update):
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def mention(user) -> str:
    name = html.escape(str(user.first_name or "Unknown"))
    return f'<a href="tg://user?id={user.id}">{name}</a>'


def cache_user(user) -> None:
    """Cache user data — MongoDB PRIMARY, SQLite secondary in-process cache.

    Write order:
      1. MongoDB (persistent, survives redeploys) — always attempted first
      2. SQLite  (in-process cache, fast reads)   — written after Mongo
    """
    if not user or getattr(user, "is_bot", False):
        return
    # 1. Primary: MongoDB
    mongo_upsert_user(user)
    # 2. Secondary: SQLite local cache (best-effort, non-critical)
    uname = getattr(user, "username", None)
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO user_cache "
            "(user_id, first_name, last_name, username, last_seen) VALUES (?,?,?,?,?)",
            (user.id, user.first_name, getattr(user, "last_name", None),
             uname.lower() if uname else None, int(time.time())),
        )
        conn.commit()
    except Exception:
        pass


def record_chat_member(chat_id: int, user) -> None:
    """Record a user as a member of chat_id — MongoDB PRIMARY, SQLite secondary.

    MongoDB is written first so data persists immediately even if the process
    is killed before SQLite flushes (e.g. Railway/Render ephemeral restarts).
    """
    if not user or getattr(user, "is_bot", False):
        return
    # 1. Primary: MongoDB (persistent)
    mongo_upsert_group_member(chat_id, user)
    # 2. Secondary: SQLite in-process cache
    uname = getattr(user, "username", None)
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO chat_members "
            "(chat_id, user_id, first_name, username, last_seen) VALUES (?,?,?,?,?)",
            (chat_id, user.id, user.first_name or "Unknown",
             uname.lower() if uname else None, int(time.time())),
        )
        conn.commit()
    except Exception:
        pass


def sqlite_find_by_username(username: str):
    """SECONDARY lookup — SQLite in-process cache only.

    Use only as a last resort after MongoDB lookup fails.
    Data here does NOT survive redeploys; MongoDB is authoritative.
    """
    clean = username.lower().lstrip("@")
    try:
        cursor.execute(
            "SELECT user_id, first_name, last_name, username "
            "FROM user_cache WHERE username=?", (clean,)
        )
        return cursor.fetchone()
    except Exception:
        return None


def sqlite_find_by_id(user_id: int):
    """SECONDARY lookup — SQLite in-process cache only.

    Use only as a last resort after MongoDB lookup fails.
    Data here does NOT survive redeploys; MongoDB is authoritative.
    """
    try:
        cursor.execute(
            "SELECT user_id, first_name, last_name, username "
            "FROM user_cache WHERE user_id=?", (user_id,)
        )
        return cursor.fetchone()
    except Exception:
        return None


class _CachedUser:
    """Minimal user-like object built from DB rows."""
    def __init__(self, user_id, first_name="Unknown", last_name=None, username=None):
        self.id         = int(user_id)
        self.first_name = first_name or "Unknown"
        self.last_name  = last_name
        self.username   = username
        self.is_bot     = False


async def get_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """4-tier user resolution — MongoDB is checked BEFORE SQLite.

    Priority order:
      1. Replied-to message  (zero-latency, always fresh)
      2. Telegram Bot API    (live, but needs active account)
      3. MongoDB             (persistent across redeploys — PRIMARY cache)
      4. SQLite              (in-process only — SECONDARY/fallback cache)
    """
    if update.message and update.message.reply_to_message:
        u = update.message.reply_to_message.from_user
        if u:
            cache_user(u)   # opportunistically refresh cache on every reply
            return u, None

    if not context.args:
        return None, "❌ Reply to a user's message or provide their @username / ID."

    raw = context.args[0].lstrip("@")

    # ── Numeric ID ────────────────────────────────────────────────────────────
    if raw.isdigit():
        uid = int(raw)

        # Tier 1: Telegram API (most up-to-date)
        try:
            user = await context.bot.get_chat(uid)
            cache_user(user)    # write to both Mongo + SQLite while we have fresh data
            return user, None
        except (BadRequest, TelegramError):
            pass

        # Tier 2: MongoDB (persistent, survives redeploys)
        doc = mongo_find_by_id(uid)
        if doc:
            return _CachedUser(
                _mongo_uid(doc), _mongo_fname(doc),
                doc.get("last_name"), _mongo_uname(doc)
            ), None

        # Tier 3: SQLite (in-process only, last resort)
        row = sqlite_find_by_id(uid)
        if row:
            return _CachedUser(*row), None

        return None, (
            f"❌ User <code>{uid}</code> not found in Telegram API or any cache.\n"
            "💡 Have them send a message in the group first, or provide their @username."
        )

    # ── @username ─────────────────────────────────────────────────────────────
    # Tier 1: Telegram API
    try:
        user = await context.bot.get_chat(f"@{raw}")
        cache_user(user)    # refresh cache with latest name/username
        return user, None
    except (BadRequest, TelegramError):
        pass

    # Tier 2: MongoDB (also searches username_history for renamed users)
    doc = mongo_find_by_username(raw)
    if doc:
        return _CachedUser(
            _mongo_uid(doc), _mongo_fname(doc),
            doc.get("last_name"), _mongo_uname(doc)
        ), None

    # Tier 3: SQLite in-process cache (fallback only)
    row = sqlite_find_by_username(raw)
    if row:
        return _CachedUser(*row), None

    return (
        None,
        f"❌ @{raw} not found in Telegram API, MongoDB, or local cache.\n"
        "💡 They must send at least one message in a monitored group first.\n"
        "Or use their numeric user ID instead.",
    )


def parse_time(raw: str):
    m = re.fullmatch(r"(\d+)([smhd])", raw.strip().lower())
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2)
    return n * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def fmt_secs(secs: int) -> str:
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    if secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    return f"{secs // 86400}d {(secs % 86400) // 3600}h"


def human_time(raw: str, secs: int) -> str:
    """Return a human-friendly string like '10 seconds', '5 minutes', '2 hours', '1 day'."""
    m = re.fullmatch(r"(\d+)([smhd])", raw.strip().lower())
    if not m:
        return fmt_secs(secs)
    n, unit = int(m.group(1)), m.group(2)
    labels = {"s": ("second", "seconds"), "m": ("minute", "minutes"),
              "h": ("hour", "hours"), "d": ("day", "days")}
    singular, plural = labels[unit]
    return f"{n} {singular if n == 1 else plural}"


async def send_action_msg(
    chat_id: int,
    text: str,
    context: ContextTypes.DEFAULT_TYPE,
    reply_to: int = None,
) -> None:
    """Send ban/kick/mute message.

    If ACTION_PHOTO_FILE_ID is set the text is sent as a photo caption,
    otherwise as a plain HTML text message.
    """
    kwargs = {"parse_mode": ParseMode.HTML}
    if reply_to:
        kwargs["reply_to_message_id"] = reply_to
    try:
        if ACTION_PHOTO_FILE_ID:
            await context.bot.send_photo(
                chat_id, photo=ACTION_PHOTO_FILE_ID, caption=text, **kwargs
            )
        else:
            await context.bot.send_message(chat_id, text, **kwargs)
    except Exception:
        try:
            await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)
        except Exception:
            pass


async def log_action(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str):
    cursor.execute("SELECT log_channel FROM settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if row and row[0]:
        try:
            await context.bot.send_message(row[0], text, parse_mode=ParseMode.HTML)
        except Exception:
            pass


def inc_stat(key: str):
    try:
        cursor.execute("INSERT OR IGNORE INTO bot_stats (key, value) VALUES (?,0)", (key,))
        cursor.execute("UPDATE bot_stats SET value=value+1 WHERE key=?", (key,))
        conn.commit()
    except Exception:
        pass
    # Also persist to MongoDB so stats survive redeploys
    if mongo_stats_col is not None:
        try:
            mongo_stats_col.update_one(
                {"key": key},
                {"$inc": {"value": 1}},
                upsert=True,
            )
        except Exception:
            pass


def get_stat(key: str) -> int:
    # Prefer MongoDB (persists across redeploys), fall back to SQLite
    if mongo_stats_col is not None:
        try:
            doc = mongo_stats_col.find_one({"key": key})
            if doc:
                return doc.get("value", 0)
        except Exception:
            pass
    cursor.execute("SELECT value FROM bot_stats WHERE key=?", (key,))
    r = cursor.fetchone()
    return r[0] if r else 0


def track_group(chat):
    if not chat:
        return
    try:
        uname = chat.username or ""
        cursor.execute(
            "INSERT OR REPLACE INTO bot_groups (chat_id, title, username, joined_at) VALUES (?,?,?,?)",
            (chat.id, chat.title or "", uname, int(time.time())),
        )
        conn.commit()
    except Exception:
        pass
    # Also persist to MongoDB so group list survives redeploys
    if mongo_groups_col is not None:
        try:
            mongo_groups_col.update_one(
                {"chat_id": chat.id},
                {"$set": {
                    "chat_id": chat.id,
                    "title": chat.title or "",
                    "username": chat.username or "",
                    "joined_at": int(time.time()),
                }},
                upsert=True,
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  HELP CONTENT
# ══════════════════════════════════════════════════════════
HELP_TEXTS = {
    "help_admin": (
        "🛡️ <b>Admin Commands</b>\n\n"
        "/ban — Ban a user\n/unban — Unban a user\n/sban — Silent ban\n"
        "/tban — Temp ban (/tban 2h)\n/kick — Kick a user\n/skick — Silent kick\n"
        "/kickme — Kick yourself\n/mute — Mute a user\n/unmute — Unmute a user\n"
        "/smute — Silent mute\n/tmute — Temp mute\n/stmute — Silent temp mute\n"
        "/promote — Promote to admin\n/demote — Demote admin\n/title — Set admin title\n"
        "/adminlist — List all admins\n/purge — Delete messages in bulk\n"
        "/spurge — Silent purge\n/del — Delete replied message\n"
        "/pin — Pin a message\n/unpin — Unpin a/all message(s)\n"
        "/invite — Generate invite link\n/report — Report to admins\n/banme — Ban yourself"
    ),
    "help_welcome": (
        "👋 <b>Welcome / Goodbye</b>\n\n"
        "/setwelcome — Set custom welcome\n/resetwelcome — Reset to default\n"
        "/welcome — Toggle welcome on/off\n/setgoodbye — Set custom goodbye\n"
        "/resetgoodbye — Reset to default\n/goodbye — Toggle goodbye on/off\n"
        "/cleanwelcome — Delete old welcome messages\n/cleanservice — Delete service messages\n\n"
        "📌 <b>Placeholders:</b>\n"
        "<code>{mention}</code> <code>{first}</code> <code>{last}</code> "
        "<code>{chat}</code> <code>{id}</code> <code>{count}</code>"
    ),
    "help_warns": (
        "⚠️ <b>Warning System</b>\n\n"
        "/warn [reason] — Warn a user\n/unwarn — Remove last warn\n"
        "/warns — Check user's warns\n/resetwarns — Reset all warns\n"
        "/setwarnlimit — Set max warns (default: 3)\n"
        "/setwarnaction — Set action: <code>ban|kick|mute</code>\n"
        "/strongwarn — Toggle strong warns on/off"
    ),
    "help_notes": (
        "📝 <b>Notes</b>\n\n"
        "/save &lt;name&gt; &lt;content&gt; — Save a note\n"
        "/get &lt;name&gt; — Retrieve a note\n#name — Quick-retrieve a note\n"
        "/clear &lt;name&gt; — Delete a note\n/clearall — Clear ALL notes\n"
        "/notes — List all saved notes"
    ),
    "help_filters": (
        "🔍 <b>Filters</b>\n\n"
        "/filter &lt;keyword&gt; &lt;response&gt; — Add a filter\n"
        "  Or reply to a message (text/photo/sticker/gif) + <code>/filter &lt;keyword&gt;</code>\n"
        "/stop &lt;keyword&gt; — Remove a filter\n/stopall — Remove ALL filters\n"
        "/filters — List all active filters\n\n"
        "Filters trigger on the keyword appearing in <b>any</b> message including captions."
    ),
    "help_locks": (
        "🔒 <b>Locks</b>\n\n"
        "/lock &lt;type|all&gt; — Lock a message type\n"
        "/unlock &lt;type|all&gt; — Unlock a message type\n"
        "/locks — Show lock status\n\n"
        "📌 <b>Types:</b>\n"
        "<code>sticker link forward photo video document audio voice gif poll</code>\n"
        "<code>contact location game inline rtl button all_media text all</code>\n\n"
        "🔐 <b>Admin Locks</b> (group owner only):\n"
        "/adminlock &lt;type&gt; — Lock type even for admins\n"
        "/adminunlock &lt;type&gt; — Remove admin lock\n"
        "/adminlocks — Show admin lock status\n"
        "Types: <code>sticker gif text url photo video document audio voice</code>"
    ),
    "help_flood": (
        "🌊 <b>Flood Control</b>\n\n"
        "/setflood &lt;n&gt; [time] — Set flood limit\n"
        "  e.g. <code>/setflood 5 10s</code> · <code>/setflood 8 1m</code>\n"
        "/setfloodaction &lt;ban|kick|mute|tmute &lt;t&gt;&gt;\n"
        "/flood — Show flood settings\n"
        "Use <code>/setflood 0</code> to disable"
    ),
    "help_blacklist": (
        "🚫 <b>Blacklist</b>\n\n"
        "/addblacklist &lt;word&gt; — Add a blacklisted word\n"
        "/rmblacklist &lt;word&gt; — Remove a blacklisted word\n"
        "/blacklist — Show all blacklisted words\n"
        "/blacklistmode &lt;del|warn|mute|kick|ban&gt; — Set action"
    ),
    "help_tagall": (
        "📣 <b>Tag All Members</b>\n\n"
        "Admins only. Send <code>@all your message</code> to tag all members 10 at a time.\n\n"
        "Or reply to a message and send <code>@all</code> — the bot forwards that message "
        "with member tags.\n\n"
        "Use /cancel or /stoptag to stop tagging mid-way."
    ),
    "help_misc": (
        "🎲 <b>Misc</b>\n\n"
        "/kickme · /banme · /afk\n"
        "/connect · /disconnect\n"
        "/gban · /ungban · /gbanlist — Global bans (owner)\n"
        "/broadcast — Broadcast (owner)\n"
        "/botstats — Bot statistics (owner)\n"
        "/botgroups — List groups (owner)\n"
        "/disable &lt;cmd&gt; · /enable &lt;cmd&gt; · /disabled\n"
        "/whitelist · /unwhitelist · /whitelisted\n"
        "/setlog — Set log channel"
    ),
}

HELP_MENU_KB = [
    [InlineKeyboardButton("🛡️ Admin",     callback_data="help_admin"),
     InlineKeyboardButton("👋 Welcome",   callback_data="help_welcome")],
    [InlineKeyboardButton("⚠️ Warns",     callback_data="help_warns"),
     InlineKeyboardButton("📝 Notes",     callback_data="help_notes")],
    [InlineKeyboardButton("🔍 Filters",   callback_data="help_filters"),
     InlineKeyboardButton("🔒 Locks",     callback_data="help_locks")],
    [InlineKeyboardButton("🌊 Flood",     callback_data="help_flood"),
     InlineKeyboardButton("🚫 Blacklist", callback_data="help_blacklist")],
    [InlineKeyboardButton("📣 Tag All",   callback_data="help_tagall"),
     InlineKeyboardButton("🎲 Misc",      callback_data="help_misc")],
]

# ══════════════════════════════════════════════════════════
#  /start  &  /help
# ══════════════════════════════════════════════════════════
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        # cache_user writes to MongoDB first, then SQLite
        cache_user(user)
        # Track DM users in SQLite (session stats — not critical to persist)
        try:
            cursor.execute(
                "INSERT OR REPLACE INTO dm_users "
                "(user_id, first_name, username, started_at) VALUES (?,?,?,?)",
                (user.id, user.first_name, user.username, int(time.time())),
            )
            conn.commit()
        except Exception:
            pass

    kb = [
        [InlineKeyboardButton("📋 Help", callback_data="help_main"),
         InlineKeyboardButton("➕ Add to Group",
                              url=f"https://t.me/{context.bot.username}?startgroup=true")],
        [InlineKeyboardButton("📌 Commands", callback_data="help_main"),
         InlineKeyboardButton("👤 My Info",  callback_data="my_info")],
    ]
    await update.message.reply_text(
        "👋 <b>Hey! I'm The Manager v1.0!</b> 🤖\n\n"
        "Your ultimate Telegram group management companion!\n\n"
        "✨ <b>Features in v1:</b>\n"
        "• <b>@all</b> — tag all members 10 at a time with emojis\n"
        "• <b>/adminlock</b> — lock content types even for admins\n"
        "• <b>/lock all</b> / <b>/unlock all</b> — lock/unlock everything at once\n"
        "• Filters trigger on images/stickers/gifs too\n"
        "• Temp mute with auto-unmute (/tmute)\n"
        "• Smarter MongoDB user lookup (multi-field search)\n\n"
        "<i>Use /help to see all commands.</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📚 <b>The Manager — Help Menu</b>\n\nPick a category:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(HELP_MENU_KB),
    )


# ══════════════════════════════════════════════════════════
#  CALLBACK ROUTER
# ══════════════════════════════════════════════════════════
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "help_main":
        await query.edit_message_text(
            "📚 <b>The Manager — Help Menu</b>\n\nPick a category:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(HELP_MENU_KB),
        )
    elif data in HELP_TEXTS:
        back_kb = [[InlineKeyboardButton("⬅️ Back", callback_data="help_main")]]
        await query.edit_message_text(
            HELP_TEXTS[data],
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(back_kb),
        )
    elif data == "my_info":
        u = query.from_user
        uname = f"@{u.username}" if u.username else "—"
        await query.edit_message_text(
            f"ℹ️ <b>Your Info:</b>\n\n"
            f"👤 Name: {html.escape(u.first_name or '')}\n"
            f"🆔 ID: <code>{u.id}</code>\n"
            f"📛 Username: {uname}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="help_main")]]
            ),
        )
    elif data.startswith("captcha_ok_") or data.startswith("cap_"):
        await captcha_button_cb(update, context)
        return
    elif data.startswith("unwarn_"):
        await _unwarn_callback(update, context)
    elif data == "rules_ack":
        await query.answer("✅ Thanks for reading the rules!", show_alert=True)
    elif data == "show_rules":
        chat_id = update.effective_chat.id
        cursor.execute("SELECT rules_text FROM rules WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        text = (row[0][:200] + "…") if row and row[0] else "No rules set yet."
        await query.answer(text, show_alert=True)
    elif data.startswith("approve_"):
        uid = int(data.split("_")[1])
        chat_id = update.effective_chat.id
        if not await is_admin(update, context):
            return await query.answer("❌ Admins only!", show_alert=True)
        cursor.execute("INSERT OR IGNORE INTO approved (chat_id, user_id) VALUES (?,?)", (chat_id, uid))
        conn.commit()
        await query.answer("✅ User approved!", show_alert=True)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  BAN / SBAN / UNBAN / KICK / SKICK / BANME / KICKME
# ══════════════════════════════════════════════════════════
@stale_guard
@admin_only
async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ I can't ban an admin!")
    if target.id == context.bot.id:
        return await update.message.reply_text("😂 I'm not going to ban myself.")
    reason_parts = context.args[1:] if context.args and not update.message.reply_to_message else (context.args or [])
    reason = " ".join(reason_parts) or "No reason provided"
    try:
        await context.bot.ban_chat_member(update.effective_chat.id, target.id)
        msg = random.choice(BAN_MSGS).format(user=html.escape(target.first_name))
        text = f"{msg}\n\n👤 {mention(target)}\n📋 Reason: <i>{html.escape(reason)}</i>"
        await send_action_msg(update.effective_chat.id, text, context)
        inc_stat("bans")
        await log_action(context, update.effective_chat.id,
                         f"🔨 <b>BAN</b>\nUser: {mention(target)} (<code>{target.id}</code>)\n"
                         f"By: {mention(update.effective_user)}\nReason: {html.escape(reason)}")
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed to ban: {e}")


@admin_only
async def sban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return
    if await is_admin(update, context, target.id):
        return
    try:
        await update.message.delete()
        if update.message.reply_to_message:
            try:
                await update.message.reply_to_message.delete()
            except BadRequest:
                pass
        await context.bot.ban_chat_member(update.effective_chat.id, target.id)
        inc_stat("bans")
    except BadRequest:
        pass


@admin_only
async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    try:
        await context.bot.unban_chat_member(update.effective_chat.id, target.id)
        await update.message.reply_text(
            f"✅ {mention(target)} has been <b>unbanned</b> and can rejoin!",
            parse_mode=ParseMode.HTML,
        )
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed to unban: {e}")


@stale_guard
@admin_only
async def kick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ I can't kick an admin!")
    try:
        chat_id = update.effective_chat.id
        await context.bot.ban_chat_member(chat_id, target.id)
        await context.bot.unban_chat_member(chat_id, target.id)
        msg = random.choice(KICK_MSGS).format(user=html.escape(target.first_name))
        text = f"{msg}\n\n👤 {mention(target)}"
        await send_action_msg(chat_id, text, context)
        inc_stat("kicks")
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed to kick: {e}")


@admin_only
async def skick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return
    if await is_admin(update, context, target.id):
        return
    try:
        await update.message.delete()
        if update.message.reply_to_message:
            try:
                await update.message.reply_to_message.delete()
            except BadRequest:
                pass
        chat_id = update.effective_chat.id
        await context.bot.ban_chat_member(chat_id, target.id)
        await context.bot.unban_chat_member(chat_id, target.id)
        inc_stat("kicks")
    except BadRequest:
        pass


async def banme_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        return await update.message.reply_text("😂 You want me to ban you from your own DM? Clever.")
    user = update.effective_user
    if await is_admin(update, context, user.id):
        return await update.message.reply_text(
            f"😂 {mention(user)}, you're an admin! Resign first.", parse_mode=ParseMode.HTML
        )
    try:
        await context.bot.ban_chat_member(update.effective_chat.id, user.id)
        roasts = [
            f"🔨 {mention(user)} wanted to be banned. Wish granted. First sensible request they've made.",
            f"💀 {mention(user)} self-destructed. We were going to do it anyway. They just beat us to it.",
            f"🧨 {mention(user)} pulled the pin on themselves. The group approves this message.",
            f"📦 {mention(user)} self-packaged, self-labeled, self-shipped. Destination: banned.",
            f"🎭 {mention(user)} requested their own ban. The dramatic exit nobody asked for.",
        ]
        await update.message.reply_text(random.choice(roasts), parse_mode=ParseMode.HTML)
    except (BadRequest, Forbidden):
        await update.message.reply_text("❌ Couldn't ban you. You're free to stay. Tragically.")


async def kickme_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        return await update.message.reply_text("😂 Kick you from your own DM? Bold.")
    user = update.effective_user
    if await is_admin(update, context, user.id):
        return await update.message.reply_text(
            f"😂 {mention(user)}, you're an admin! The only way out is resignation.",
            parse_mode=ParseMode.HTML,
        )
    try:
        await context.bot.ban_chat_member(update.effective_chat.id, user.id)
        await context.bot.unban_chat_member(update.effective_chat.id, user.id)
        await update.message.reply_text(
            random.choice(KICKME_ROASTS).format(mention=mention(user)),
            parse_mode=ParseMode.HTML,
        )
    except (BadRequest, Forbidden):
        fail_roasts = [
            f"😂 I tried kicking {mention(user)} but they're load-bearing. The group needs them. Sadly.",
            f"💀 {mention(user)} tried to leave. Telegram said no. I'm as disappointed as they are.",
        ]
        await update.message.reply_text(random.choice(fail_roasts), parse_mode=ParseMode.HTML)


# ══════════════════════════════════════════════════════════
#  MUTE / UNMUTE / SMUTE / TMUTE / STMUTE
# ══════════════════════════════════════════════════════════
@stale_guard
@admin_only
async def mute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ I can't mute an admin!")
    try:
        await context.bot.restrict_chat_member(
            update.effective_chat.id, target.id,
            permissions=ChatPermissions(can_send_messages=False),
        )
        msg = random.choice(MUTE_MSGS).format(user=html.escape(target.first_name))
        text = f"{msg}\n\n👤 {mention(target)}"
        await send_action_msg(update.effective_chat.id, text, context)
        inc_stat("mutes")
    except BadRequest as e:
        err_str = str(e).lower()
        if "not enough rights" in err_str or "administrator" in err_str:
            await update.message.reply_text("❌ I need Restrict Members permission!")
        else:
            await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def unmute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    try:
        await context.bot.restrict_chat_member(update.effective_chat.id, target.id, FULL_PERMS)
        msg = random.choice(UNMUTE_MSGS).format(user=html.escape(target.first_name))
        await update.message.reply_text(f"{msg}\n\n👤 {mention(target)}", parse_mode=ParseMode.HTML)
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def smute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return
    if await is_admin(update, context, target.id):
        return
    try:
        if update.message.reply_to_message:
            try:
                await update.message.reply_to_message.delete()
            except BadRequest:
                pass
        try:
            await update.message.delete()
        except BadRequest:
            pass
        await context.bot.restrict_chat_member(
            update.effective_chat.id, target.id,
            ChatPermissions(can_send_messages=False),
        )
        inc_stat("mutes")
    except BadRequest:
        pass


async def _do_unmute(context: ContextTypes.DEFAULT_TYPE):
    chat_id, user_id, first_name = context.job.data
    try:
        await context.bot.restrict_chat_member(chat_id, user_id, FULL_PERMS)
        await context.bot.send_message(
            chat_id,
            f"🔊 <b>{html.escape(first_name)}</b>'s temp-mute has expired — welcome back!",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


@admin_only
async def tmute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ I can't mute an admin!")
    raw = None
    for arg in reversed(context.args or []):
        if parse_time(arg) is not None:
            raw = arg
            break
    secs = parse_time(raw) if raw else None
    if not secs:
        return await update.message.reply_text(
            "❌ Please provide a duration!\n"
            "Examples: <code>/tmute 10s</code> · <code>/tmute 5m</code> · "
            "<code>/tmute 2h</code> · <code>/tmute 1d</code>",
            parse_mode=ParseMode.HTML,
        )
    # Telegram API treats until_date < 30 seconds as permanent — use max(secs, 30)
    tg_secs = max(secs, 30)
    until = datetime.utcnow() + timedelta(seconds=tg_secs)
    # Show full date+time for mutes longer than 1 hour, otherwise just time
    if tg_secs >= 3600:
        unmute_at_str = until.strftime('%Y-%m-%d %H:%M UTC')
    else:
        unmute_at_str = until.strftime('%H:%M:%S UTC')
    try:
        await context.bot.restrict_chat_member(
            update.effective_chat.id, target.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until,
        )
        # Always schedule a job so short durations (<30s) also unmute reliably
        context.application.job_queue.run_once(
            _do_unmute, when=secs,
            data=(update.effective_chat.id, target.id, target.first_name),
        )
        duration_str = human_time(raw, secs)
        roast = random.choice(MUTE_MSGS).format(user=html.escape(target.first_name))
        text = (
            f"{roast}\n\n"
            f"👤 {mention(target)}\n"
            f"⏱️ Muted for <b>{duration_str}</b>\n"
            f"🔇 Will be unmuted at <b>{unmute_at_str}</b>"
        )
        await send_action_msg(update.effective_chat.id, text, context)
        inc_stat("mutes")
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def stmute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return
    if await is_admin(update, context, target.id):
        return
    raw = None
    for arg in reversed(context.args or []):
        if parse_time(arg) is not None:
            raw = arg
            break
    secs = parse_time(raw) if raw else None
    if not secs:
        return
    until = datetime.now() + timedelta(seconds=secs)
    try:
        try:
            await update.message.delete()
        except BadRequest:
            pass
        await context.bot.restrict_chat_member(
            update.effective_chat.id, target.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until,
        )
        context.application.job_queue.run_once(
            _do_unmute, when=secs,
            data=(update.effective_chat.id, target.id, target.first_name),
        )
    except BadRequest:
        pass


# ══════════════════════════════════════════════════════════
#  TBAN
# ══════════════════════════════════════════════════════════
@admin_only
async def tban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ I can't ban an admin!")
    raw = context.args[-1] if context.args else None
    secs = parse_time(raw) if raw else None
    if not secs:
        return await update.message.reply_text("❌ Provide a time! E.g. /tban @user 1h")
    until = datetime.now() + timedelta(seconds=secs)
    try:
        await context.bot.ban_chat_member(update.effective_chat.id, target.id, until_date=until)
        await update.message.reply_text(
            f"⏳ {mention(target)} banned for <b>{raw}</b>!\n"
            f"<i>Auto-unbanned at {until.strftime('%H:%M:%S')}</i>",
            parse_mode=ParseMode.HTML,
        )
        inc_stat("bans")
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed: {e}")


# ══════════════════════════════════════════════════════════
#  GLOBAL BAN (owner only)
# ══════════════════════════════════════════════════════════
@owner_only
async def gban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if target.id == OWNER_ID:
        return await update.message.reply_text("You can't gban yourself.")
    reason_parts = context.args[1:] if context.args and not update.message.reply_to_message else (context.args or [])
    reason = " ".join(reason_parts) or "No reason provided"
    gban_user(target.id, reason, update.effective_user.id)
    msg = random.choice(GBAN_MSGS).format(user=html.escape(target.first_name))
    await update.message.reply_text(
        f"{msg}\n\n👤 {mention(target)} (<code>{target.id}</code>)\n"
        f"📋 Reason: <i>{html.escape(reason)}</i>",
        parse_mode=ParseMode.HTML,
    )
    inc_stat("gbans")


@owner_only
async def ungban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if ungban_user(target.id):
        msg = random.choice(UNGBAN_MSGS).format(user=html.escape(target.first_name))
        await update.message.reply_text(f"{msg}\n\n👤 {mention(target)}", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"ℹ️ {mention(target)} is not globally banned.", parse_mode=ParseMode.HTML
        )


@owner_only
async def gbanlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if mongo_gbans_col is None:
        return await update.message.reply_text("❌ MongoDB not connected.")
    try:
        docs = list(mongo_gbans_col.find({}, {"user_id": 1, "id": 1, "reason": 1}).limit(50))
    except Exception:
        return await update.message.reply_text("❌ MongoDB query failed.")
    if not docs:
        return await update.message.reply_text("✅ Global ban list is empty!")
    lines = []
    for d in docs:
        uid = _mongo_uid(d) or "?"
        lines.append(f"• <code>{uid}</code> — {html.escape(d.get('reason',''))[:60]}")
    await update.message.reply_text(
        f"🌐 <b>Global Ban List</b> ({len(docs)}):\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  WARN SYSTEM
# ══════════════════════════════════════════════════════════
@admin_only
async def warn_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ You can't warn an admin!")
    chat_id = update.effective_chat.id
    reason_parts = context.args[1:] if context.args and not update.message.reply_to_message else (context.args or [])
    reason = " ".join(reason_parts) or "No reason provided"
    cursor.execute("SELECT max_warns, warn_action, strong_warn FROM settings WHERE chat_id=?", (chat_id,))
    s = cursor.fetchone()
    max_w, action, strong_warn = (s[0], s[1], bool(s[2])) if s else (3, "ban", False)
    cursor.execute("SELECT count, reasons FROM warns WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    row = cursor.fetchone()
    count   = (row[0] + 1) if row else 1
    reasons = (row[1] + f"\n{count}. {reason}") if (row and row[1]) else f"1. {reason}"
    cursor.execute(
        "INSERT OR REPLACE INTO warns (chat_id, user_id, count, reasons) VALUES (?,?,?,?)",
        (chat_id, target.id, count, reasons),
    )
    conn.commit()
    remaining = max(0, max_w - count)
    msg = random.choice(WARN_MSGS).format(
        user=html.escape(target.first_name), count=count, max=max_w, remaining=remaining
    )
    if count >= max_w or strong_warn:
        cursor.execute("DELETE FROM warns WHERE chat_id=? AND user_id=?", (chat_id, target.id))
        conn.commit()
        if action == "ban":
            await context.bot.ban_chat_member(chat_id, target.id)
            action_txt = f"🔨 Max warnings reached! <b>{html.escape(target.first_name)}</b> has been <b>banned</b>."
        elif action == "kick":
            await context.bot.ban_chat_member(chat_id, target.id)
            await context.bot.unban_chat_member(chat_id, target.id)
            action_txt = f"👢 Max warnings reached! <b>{html.escape(target.first_name)}</b> has been <b>kicked</b>."
        else:
            await context.bot.restrict_chat_member(chat_id, target.id, ChatPermissions(can_send_messages=False))
            action_txt = f"🔇 Max warnings reached! <b>{html.escape(target.first_name)}</b> has been <b>muted</b>."
        await update.message.reply_text(f"{msg}\n\n{action_txt}", parse_mode=ParseMode.HTML)
        inc_stat("warns")
        await log_action(context, chat_id,
                         f"⚠️ <b>WARN→{action.upper()}</b>\nUser: {mention(target)}\nReason: {html.escape(reason)}")
    else:
        kb = [[InlineKeyboardButton("🗑️ Remove Last Warn", callback_data=f"unwarn_{target.id}")]]
        await update.message.reply_text(
            f"{msg}\n\n👤 {mention(target)}\n📋 Reason: <i>{html.escape(reason)}</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )
        inc_stat("warns")


async def _unwarn_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update, context):
        return await query.answer("❌ Admins only!", show_alert=True)
    user_id = int(query.data.split("_")[1])
    chat_id = update.effective_chat.id
    cursor.execute("SELECT count FROM warns WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cursor.fetchone()
    if not row or row[0] == 0:
        return await query.answer("No warnings to remove!", show_alert=True)
    new_count = max(0, row[0] - 1)
    cursor.execute("UPDATE warns SET count=? WHERE chat_id=? AND user_id=?", (new_count, chat_id, user_id))
    conn.commit()
    await query.answer(f"✅ Warning removed. Now at {new_count}.")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass


@admin_only
async def unwarn_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    chat_id = update.effective_chat.id
    cursor.execute("SELECT count FROM warns WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    row = cursor.fetchone()
    if not row or row[0] == 0:
        return await update.message.reply_text(
            f"ℹ️ {mention(target)} has no warnings.", parse_mode=ParseMode.HTML
        )
    new_count = max(0, row[0] - 1)
    cursor.execute("UPDATE warns SET count=? WHERE chat_id=? AND user_id=?", (new_count, chat_id, target.id))
    conn.commit()
    await update.message.reply_text(
        f"✅ One warning removed from {mention(target)}. Now at <code>{new_count}</code>.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def warns_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, _ = await get_target(update, context)
    if not target:
        target = update.effective_user
    chat_id = update.effective_chat.id
    cursor.execute("SELECT count, reasons FROM warns WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    row = cursor.fetchone()
    cursor.execute("SELECT max_warns FROM settings WHERE chat_id=?", (chat_id,))
    s = cursor.fetchone()
    max_w = s[0] if s else 3
    if not row or row[0] == 0:
        return await update.message.reply_text(
            f"✅ <b>{html.escape(target.first_name)}</b> has no warnings!", parse_mode=ParseMode.HTML
        )
    await update.message.reply_text(
        f"⚠️ <b>Warnings for {mention(target)}:</b>\n\n"
        f"Count: <code>{row[0]}/{max_w}</code>\n\n<b>Reasons:</b>\n{html.escape(row[1])}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def reset_warns_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    cursor.execute("DELETE FROM warns WHERE chat_id=? AND user_id=?", (update.effective_chat.id, target.id))
    conn.commit()
    await update.message.reply_text(
        f"✅ Warnings for {mention(target)} reset!", parse_mode=ParseMode.HTML
    )


@admin_only
async def set_warn_limit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Usage: /setwarnlimit <number>")
    limit = max(1, int(context.args[0]))
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE settings SET max_warns=? WHERE chat_id=?", (limit, chat_id))
    conn.commit()
    await update.message.reply_text(f"✅ Warn limit set to <b>{limit}</b>!", parse_mode=ParseMode.HTML)


@admin_only
async def set_warn_action_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] not in ("ban", "kick", "mute"):
        return await update.message.reply_text("Usage: /setwarnaction <ban|kick|mute>")
    action = context.args[0]
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE settings SET warn_action=? WHERE chat_id=?", (action, chat_id))
    conn.commit()
    await update.message.reply_text(f"✅ Warn action set to <b>{action}</b>!", parse_mode=ParseMode.HTML)


@admin_only
async def strongwarn_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT strong_warn FROM settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    new_val = 0 if (row and row[0]) else 1
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE settings SET strong_warn=? WHERE chat_id=?", (new_val, chat_id))
    conn.commit()
    status = "enabled ✅" if new_val else "disabled ❌"
    await update.message.reply_text(
        f"💪 Strong warn: <b>{status}</b>!\n"
        f"{'Users acted on immediately on first warn.' if new_val else 'Normal warning mode restored.'}",
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  NOTES
# ══════════════════════════════════════════════════════════
@admin_only
async def save_note_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message and context.args:
        name = context.args[0].lower()
        replied = update.message.reply_to_message
        content = replied.text or replied.caption or None
        if not content:
            return await update.message.reply_text("❌ Replied message has no text/caption!")
    elif len(context.args) >= 2:
        name    = context.args[0].lower()
        content = " ".join(context.args[1:])
    else:
        return await update.message.reply_text(
            "Usage: /save &lt;name&gt; &lt;content&gt; — or reply to a message with /save &lt;name&gt;",
            parse_mode=ParseMode.HTML,
        )
    cursor.execute(
        "INSERT OR REPLACE INTO notes (chat_id, name, content) VALUES (?,?,?)",
        (update.effective_chat.id, name, content),
    )
    conn.commit()
    await update.message.reply_text(f"📝 Note <code>#{name}</code> saved!", parse_mode=ParseMode.HTML)


async def get_note_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /get <name>")
    await _send_note(update, context, context.args[0].lower())


async def _send_note(update: Update, context: ContextTypes.DEFAULT_TYPE, name: str):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT content, is_private FROM notes WHERE chat_id=? AND name=?", (chat_id, name))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text(
            f"❌ No note named <code>#{name}</code>!", parse_mode=ParseMode.HTML
        )
    content, is_private = row
    if is_private and update.effective_chat.type != "private":
        try:
            await context.bot.send_message(
                update.effective_user.id,
                f"📝 <b>#{name}:</b>\n\n{content}",
                parse_mode=ParseMode.HTML,
            )
            await update.message.reply_text("📩 Note sent to your PM!")
        except Forbidden:
            await update.message.reply_text("❌ Start a conversation with me first!")
    else:
        await update.message.reply_text(f"📝 <b>#{name}:</b>\n\n{content}", parse_mode=ParseMode.HTML)


@admin_only
async def clear_note_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /clear <name>")
    name = context.args[0].lower()
    cursor.execute("DELETE FROM notes WHERE chat_id=? AND name=?", (update.effective_chat.id, name))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"🗑️ Note <code>#{name}</code> deleted!", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ Note <code>#{name}</code> not found!", parse_mode=ParseMode.HTML)


@admin_only
async def clearall_notes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("DELETE FROM notes WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("🗑️ All notes cleared!")


async def list_notes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT name FROM notes WHERE chat_id=?", (update.effective_chat.id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📭 No notes saved here yet!")
    note_list = "\n".join(f"• <code>#{r[0]}</code>" for r in rows)
    await update.message.reply_text(
        f"📚 <b>Saved notes ({len(rows)}):</b>\n\n{note_list}\n\n"
        "<i>Use /get &lt;name&gt; or #name to retrieve</i>",
        parse_mode=ParseMode.HTML,
    )


async def check_hashtag_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    if text.startswith("#") and len(text) > 1:
        word = text.split()[0][1:].lower()
        if word:
            await _send_note(update, context, word)


# ══════════════════════════════════════════════════════════
#  FILTERS  — fixed: triggers on captions/media too
# ══════════════════════════════════════════════════════════
@admin_only
async def add_filter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message and context.args:
        keyword = context.args[0].lower()
        replied = update.message.reply_to_message
        caption = replied.caption or ""

        if replied.sticker:
            response = json.dumps({"type": "sticker", "file_id": replied.sticker.file_id})
        elif replied.animation:
            response = json.dumps({"type": "gif", "file_id": replied.animation.file_id, "caption": caption})
        elif replied.photo:
            response = json.dumps({"type": "photo", "file_id": replied.photo[-1].file_id, "caption": caption})
        elif replied.video:
            response = json.dumps({"type": "video", "file_id": replied.video.file_id, "caption": caption})
        elif replied.audio:
            response = json.dumps({"type": "audio", "file_id": replied.audio.file_id, "caption": caption})
        elif replied.voice:
            response = json.dumps({"type": "voice", "file_id": replied.voice.file_id, "caption": caption})
        elif replied.document:
            response = json.dumps({"type": "document", "file_id": replied.document.file_id, "caption": caption})
        elif replied.text:
            response = replied.text
        else:
            return await update.message.reply_text("❌ Unsupported message type for filter!")

    elif len(context.args) >= 2:
        keyword  = context.args[0].lower()
        response = " ".join(context.args[1:])
    else:
        return await update.message.reply_text(
            "Usage:\n"
            "• <code>/filter &lt;keyword&gt; &lt;response&gt;</code>\n"
            "• Reply to a message + <code>/filter &lt;keyword&gt;</code>\n"
            "Works on text, photos, stickers, GIFs, video, audio, documents.",
            parse_mode=ParseMode.HTML,
        )
    cursor.execute(
        "INSERT OR REPLACE INTO filters (chat_id, keyword, response) VALUES (?,?,?)",
        (update.effective_chat.id, keyword, response),
    )
    conn.commit()
    await update.message.reply_text(
        f"✅ Filter <code>{html.escape(keyword)}</code> added!", parse_mode=ParseMode.HTML
    )


@admin_only
async def stop_filter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /stop <keyword>")
    keyword = context.args[0].lower()
    cursor.execute("DELETE FROM filters WHERE chat_id=? AND keyword=?", (update.effective_chat.id, keyword))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(
            f"🗑️ Filter <code>{html.escape(keyword)}</code> removed!", parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            f"❌ No filter for <code>{html.escape(keyword)}</code>!", parse_mode=ParseMode.HTML
        )


@admin_only
async def stopall_filters_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("DELETE FROM filters WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("🗑️ All filters removed!")


async def list_filters_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT keyword FROM filters WHERE chat_id=?", (update.effective_chat.id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📭 No active filters!")
    lst = "\n".join(f"• <code>{html.escape(r[0])}</code>" for r in rows)
    await update.message.reply_text(
        f"🔍 <b>Active Filters ({len(rows)}):</b>\n\n{lst}", parse_mode=ParseMode.HTML
    )


async def process_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Trigger filters on any message — text, caption (images/stickers/gifs/video/etc.)."""
    if not update.message:
        return
    if update.effective_chat.type == "private":
        return
    msg  = update.message
    text = (msg.text or msg.caption or "").lower()
    if not text:
        return
    cursor.execute("SELECT keyword, response FROM filters WHERE chat_id=?", (update.effective_chat.id,))
    for keyword, response in cursor.fetchall():
        if keyword in text:
            # Try to parse as media JSON
            try:
                data = json.loads(response)
                ftype   = data.get("type")
                file_id = data.get("file_id")
                caption = data.get("caption") or ""
                if ftype == "sticker":
                    await msg.reply_sticker(file_id)
                elif ftype == "photo":
                    await msg.reply_photo(file_id, caption=caption or None, parse_mode=ParseMode.HTML)
                elif ftype == "gif":
                    await msg.reply_animation(file_id, caption=caption or None, parse_mode=ParseMode.HTML)
                elif ftype == "video":
                    await msg.reply_video(file_id, caption=caption or None, parse_mode=ParseMode.HTML)
                elif ftype == "audio":
                    await msg.reply_audio(file_id, caption=caption or None, parse_mode=ParseMode.HTML)
                elif ftype == "voice":
                    await msg.reply_voice(file_id, caption=caption or None, parse_mode=ParseMode.HTML)
                elif ftype == "document":
                    await msg.reply_document(file_id, caption=caption or None, parse_mode=ParseMode.HTML)
                else:
                    await msg.reply_text(response, parse_mode=ParseMode.HTML)
            except (json.JSONDecodeError, KeyError):
                # Plain text response
                await msg.reply_text(response, parse_mode=ParseMode.HTML)
            break


# ══════════════════════════════════════════════════════════
#  RULES
# ══════════════════════════════════════════════════════════
async def rules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT rules_text, private_rules FROM rules WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return await update.message.reply_text(
            "📭 No rules set yet! Ask an admin to set them with /setrules."
        )
    rules_text, is_private = row
    if is_private and update.effective_chat.type != "private":
        try:
            await context.bot.send_message(
                update.effective_user.id,
                f"📜 <b>Group Rules for {html.escape(update.effective_chat.title or 'this group')}:</b>\n\n{rules_text}",
                parse_mode=ParseMode.HTML,
            )
            await update.message.reply_text("📩 Rules sent to your PM!")
        except Forbidden:
            kb = [[InlineKeyboardButton("📜 Read Rules", callback_data="show_rules")]]
            await update.message.reply_text(
                f"📜 <b>Group Rules:</b>\n\n{rules_text}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(kb),
            )
    else:
        kb = [[InlineKeyboardButton("✅ I've Read the Rules", callback_data="rules_ack")]]
        await update.message.reply_text(
            f"📜 <b>Group Rules:</b>\n\n{rules_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )


@admin_only
async def set_rules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or update.message.reply_to_message.caption
        if not text:
            return await update.message.reply_text("❌ Replied message has no text!")
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text(
            "Usage: /setrules <text> or reply to a message with /setrules"
        )
    cursor.execute("INSERT OR IGNORE INTO rules (chat_id) VALUES (?)", (update.effective_chat.id,))
    cursor.execute("UPDATE rules SET rules_text=? WHERE chat_id=?", (text, update.effective_chat.id))
    conn.commit()
    await update.message.reply_text("✅ Rules updated!")


@admin_only
async def clear_rules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("DELETE FROM rules WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("🗑️ Rules cleared!")


@admin_only
async def privaterules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT private_rules FROM rules WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    new_val = 0 if (row and row[0]) else 1
    cursor.execute("INSERT OR IGNORE INTO rules (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE rules SET private_rules=? WHERE chat_id=?", (new_val, chat_id))
    conn.commit()
    status = "enabled ✅ (rules sent via PM)" if new_val else "disabled ❌ (rules shown in chat)"
    await update.message.reply_text(f"Private rules: {status}")


# ══════════════════════════════════════════════════════════
#  WELCOME / GOODBYE
# ══════════════════════════════════════════════════════════
async def on_member_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.new_chat_members:
        return
    chat_id = update.effective_chat.id
    track_group(update.effective_chat)
    chat_nm = html.escape(update.effective_chat.title or "this group")

    cursor.execute(
        "SELECT welcome_text, welcome_enabled, clean_welcome, clean_service, last_welcome_id "
        "FROM welcome WHERE chat_id=?", (chat_id,)
    )
    row = cursor.fetchone()

    if row and not row[1]:
        return

    if row and row[3]:
        try:
            await update.message.delete()
        except BadRequest:
            pass
        return

    for member in update.message.new_chat_members:
        if member.is_bot:
            cursor.execute("SELECT antibot FROM settings WHERE chat_id=?", (chat_id,))
            ab = cursor.fetchone()
            if ab and ab[0]:
                try:
                    await context.bot.ban_chat_member(chat_id, member.id)
                    await context.bot.send_message(
                        chat_id,
                        f"🤖 Bot <b>{html.escape(member.first_name)}</b> auto-removed! (Anti-bot ON)",
                        parse_mode=ParseMode.HTML,
                    )
                except BadRequest:
                    pass
            continue

        cache_user(member)
        record_chat_member(chat_id, member)
        men   = mention(member)
        count = await update.effective_chat.get_member_count()

        if row and row[0]:
            text = (row[0]
                    .replace("{mention}", men)
                    .replace("{first}",   html.escape(member.first_name or ""))
                    .replace("{last}",    html.escape(member.last_name  or ""))
                    .replace("{chat}",    chat_nm)
                    .replace("{id}",      str(member.id))
                    .replace("{count}",   str(count)))
        else:
            text = random.choice(WELCOME_MSGS).format(mention=men, chat=chat_nm)

        if row and row[2] and row[4]:
            try:
                await context.bot.delete_message(chat_id, row[4])
            except BadRequest:
                pass

        kb = [[InlineKeyboardButton("📜 Read Rules", callback_data="show_rules")]]
        sent = await context.bot.send_message(
            chat_id, text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )

        if row and row[2]:
            cursor.execute("INSERT OR IGNORE INTO welcome (chat_id) VALUES (?)", (chat_id,))
            cursor.execute("UPDATE welcome SET last_welcome_id=? WHERE chat_id=?", (sent.message_id, chat_id))
            conn.commit()

        gban_doc = is_gbanned(member.id)
        if gban_doc:
            try:
                await context.bot.ban_chat_member(chat_id, member.id)
                await context.bot.send_message(
                    chat_id,
                    f"⚠️ Globally banned user {men} tried to join and was auto-removed!\n"
                    f"Reason: <i>{html.escape(gban_doc.get('reason',''))}</i>",
                    parse_mode=ParseMode.HTML,
                )
            except BadRequest:
                pass


async def on_member_left(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.left_chat_member:
        return
    member  = update.message.left_chat_member
    if member.is_bot:
        return
    chat_id = update.effective_chat.id
    cursor.execute(
        "SELECT goodbye_text, goodbye_enabled, clean_service FROM welcome WHERE chat_id=?", (chat_id,)
    )
    row = cursor.fetchone()
    if row and row[2]:
        try:
            await update.message.delete()
        except BadRequest:
            pass
        return
    if row and not row[1]:
        return
    men     = mention(member)
    chat_nm = html.escape(update.effective_chat.title or "this group")
    if row and row[0]:
        text = (row[0]
                .replace("{mention}", men)
                .replace("{first}",   html.escape(member.first_name or ""))
                .replace("{chat}",    chat_nm))
    else:
        text = random.choice(GOODBYE_MSGS).format(mention=men, chat=chat_nm)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


@admin_only
async def set_welcome_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or update.message.reply_to_message.caption
        if not text:
            return await update.message.reply_text("❌ Replied message has no text!")
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text(
            "Usage: <code>/setwelcome &lt;text&gt;</code> or reply to a message\n"
            "Placeholders: <code>{mention} {first} {last} {chat} {id} {count}</code>",
            parse_mode=ParseMode.HTML,
        )
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO welcome (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE welcome SET welcome_text=? WHERE chat_id=?", (text, chat_id))
    conn.commit()
    await update.message.reply_text("✅ Welcome message updated!")


@admin_only
async def set_goodbye_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or update.message.reply_to_message.caption
        if not text:
            return await update.message.reply_text("❌ Replied message has no text!")
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text(
            "Usage: /setgoodbye <text>\nPlaceholders: {mention} {first} {chat}",
            parse_mode=ParseMode.HTML,
        )
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO welcome (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE welcome SET goodbye_text=? WHERE chat_id=?", (text, chat_id))
    conn.commit()
    await update.message.reply_text("✅ Goodbye message updated!")


@admin_only
async def reset_welcome_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("UPDATE welcome SET welcome_text=NULL WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("✅ Welcome message reset to default!")


@admin_only
async def reset_goodbye_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("UPDATE welcome SET goodbye_text=NULL WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("✅ Goodbye message reset to default!")


@admin_only
async def toggle_welcome_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT welcome_enabled FROM welcome WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    new_val = 0 if (row and row[0]) else 1
    cursor.execute("INSERT OR IGNORE INTO welcome (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE welcome SET welcome_enabled=? WHERE chat_id=?", (new_val, chat_id))
    conn.commit()
    await update.message.reply_text(f"Welcome: {'enabled ✅' if new_val else 'disabled ❌'}")


@admin_only
async def toggle_goodbye_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT goodbye_enabled FROM welcome WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    new_val = 0 if (row and row[0]) else 1
    cursor.execute("INSERT OR IGNORE INTO welcome (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE welcome SET goodbye_enabled=? WHERE chat_id=?", (new_val, chat_id))
    conn.commit()
    await update.message.reply_text(f"Goodbye: {'enabled ✅' if new_val else 'disabled ❌'}")


@admin_only
async def cleanwelcome_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT clean_welcome FROM welcome WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    new_val = 0 if (row and row[0]) else 1
    cursor.execute("INSERT OR IGNORE INTO welcome (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE welcome SET clean_welcome=? WHERE chat_id=?", (new_val, chat_id))
    conn.commit()
    status = "enabled ✅ (old welcome messages deleted)" if new_val else "disabled ❌"
    await update.message.reply_text(f"Clean welcome: {status}")


@admin_only
async def cleanservice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT clean_service FROM welcome WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    new_val = 0 if (row and row[0]) else 1
    cursor.execute("INSERT OR IGNORE INTO welcome (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE welcome SET clean_service=? WHERE chat_id=?", (new_val, chat_id))
    conn.commit()
    status = "enabled ✅ (join/leave messages deleted)" if new_val else "disabled ❌"
    await update.message.reply_text(f"Clean service messages: {status}")


# ══════════════════════════════════════════════════════════
#  LOCKS  — fixed: support "all" keyword, added "text" type
# ══════════════════════════════════════════════════════════
LOCK_COLS = [
    "sticker", "link", "forward", "photo", "video", "document",
    "audio", "voice", "gif", "poll", "contact", "location",
    "game", "inline", "rtl", "button", "all_media", "text",
]
LOCK_ALL_TYPES = set(LOCK_COLS)


@admin_only
async def lock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text(
            f"Usage: /lock &lt;type|all&gt;\nTypes: <code>{' '.join(LOCK_COLS)}</code>",
            parse_mode=ParseMode.HTML,
        )
    t = context.args[0].lower()
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO locks (chat_id) VALUES (?)", (chat_id,))

    if t == "all":
        sets = ", ".join(f"{col}=1" for col in LOCK_COLS)
        cursor.execute(f"UPDATE locks SET {sets} WHERE chat_id=?", (chat_id,))
        cursor.execute("INSERT OR IGNORE INTO adminlocks (chat_id) VALUES (?)", (chat_id,))
        admin_sets = ", ".join(f"{col}=1" for col in ADMINLOCK_COLS)
        cursor.execute(f"UPDATE adminlocks SET {admin_sets} WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text("🔒 <b>All</b> message types locked!", parse_mode=ParseMode.HTML)
    elif t in LOCK_ALL_TYPES:
        cursor.execute(f"UPDATE locks SET {t}=1 WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text(f"🔒 <b>{t}</b> is now locked!", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"❌ Unknown type. Available: <code>{' '.join(LOCK_COLS)}</code> or <code>all</code>",
            parse_mode=ParseMode.HTML,
        )


@admin_only
async def unlock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text(
            f"Usage: /unlock &lt;type|all&gt;\nTypes: <code>{' '.join(LOCK_COLS)}</code>",
            parse_mode=ParseMode.HTML,
        )
    t = context.args[0].lower()
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO locks (chat_id) VALUES (?)", (chat_id,))

    if t == "all":
        sets = ", ".join(f"{col}=0" for col in LOCK_COLS)
        cursor.execute(f"UPDATE locks SET {sets} WHERE chat_id=?", (chat_id,))
        cursor.execute("INSERT OR IGNORE INTO adminlocks (chat_id) VALUES (?)", (chat_id,))
        admin_sets = ", ".join(f"{col}=0" for col in ADMINLOCK_COLS)
        cursor.execute(f"UPDATE adminlocks SET {admin_sets} WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text("🔓 <b>All</b> locks removed!", parse_mode=ParseMode.HTML)
    elif t in LOCK_ALL_TYPES:
        cursor.execute(f"UPDATE locks SET {t}=0 WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text(f"🔓 <b>{t}</b> is now unlocked!", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"❌ Unknown type. Available: <code>{' '.join(LOCK_COLS)}</code> or <code>all</code>",
            parse_mode=ParseMode.HTML,
        )


@admin_only
async def locks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT * FROM locks WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("🔓 No locks configured. All types allowed.")
    lines = [f"{'🔒' if row[i + 1] else '🔓'} {t}" for i, t in enumerate(LOCK_COLS)]
    mid  = len(lines) // 2
    await update.message.reply_text(
        f"🔒 <b>Lock Status:</b>\n\n"
        f"{chr(10).join(lines[:mid])}\n\n{chr(10).join(lines[mid:])}",
        parse_mode=ParseMode.HTML,
    )


# ── ADMINLOCK — locks types even for admins (group owner only) ──
ADMINLOCK_COLS = ["sticker", "gif", "text", "url", "photo", "video", "document", "audio", "voice"]


async def adminlock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        return await update.message.reply_text("⚠️ Groups only!")
    if not await is_group_owner(update, context):
        return await update.message.reply_text(
            "🔒 Only the <b>group owner</b> can use admin locks!", parse_mode=ParseMode.HTML
        )
    if not context.args:
        return await update.message.reply_text(
            f"Usage: /adminlock &lt;type&gt;\nTypes: <code>{' '.join(ADMINLOCK_COLS)}</code>",
            parse_mode=ParseMode.HTML,
        )
    t = context.args[0].lower()
    if t not in ADMINLOCK_COLS:
        return await update.message.reply_text(
            f"❌ Unknown type. Available: <code>{' '.join(ADMINLOCK_COLS)}</code>",
            parse_mode=ParseMode.HTML,
        )
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO adminlocks (chat_id) VALUES (?)", (chat_id,))
    cursor.execute(f"UPDATE adminlocks SET {t}=1 WHERE chat_id=?", (chat_id,))
    conn.commit()
    await update.message.reply_text(
        f"🔐 Admin lock on <b>{t}</b> enabled!\n"
        f"Even admins cannot send this type now.",
        parse_mode=ParseMode.HTML,
    )


async def adminunlock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        return await update.message.reply_text("⚠️ Groups only!")
    if not await is_group_owner(update, context):
        return await update.message.reply_text(
            "🔒 Only the <b>group owner</b> can modify admin locks!", parse_mode=ParseMode.HTML
        )
    if not context.args:
        return await update.message.reply_text(
            f"Usage: /adminunlock &lt;type&gt;\nTypes: <code>{' '.join(ADMINLOCK_COLS)}</code>",
            parse_mode=ParseMode.HTML,
        )
    t = context.args[0].lower()
    if t not in ADMINLOCK_COLS:
        return await update.message.reply_text(
            f"❌ Unknown type. Available: <code>{' '.join(ADMINLOCK_COLS)}</code>",
            parse_mode=ParseMode.HTML,
        )
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO adminlocks (chat_id) VALUES (?)", (chat_id,))
    cursor.execute(f"UPDATE adminlocks SET {t}=0 WHERE chat_id=?", (chat_id,))
    conn.commit()
    await update.message.reply_text(
        f"🔓 Admin lock on <b>{t}</b> removed!", parse_mode=ParseMode.HTML
    )


async def adminlocks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        return await update.message.reply_text("⚠️ Groups only!")
    chat_id = update.effective_chat.id
    cursor.execute("SELECT * FROM adminlocks WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("🔓 No admin locks configured.")
    lines = [f"{'🔐' if row[i + 1] else '🔓'} {t}" for i, t in enumerate(ADMINLOCK_COLS)]
    await update.message.reply_text(
        "🔐 <b>Admin Lock Status</b>\n<i>(applies to admins too)</i>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


async def enforce_admin_locks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enforce admin locks — runs after regular lock check, applies even to admins."""
    if not update.message or update.effective_chat.type == "private":
        return
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # Owner is exempt
    try:
        member = await update.effective_chat.get_member(user_id)
        if member.status == ChatMemberStatus.OWNER:
            return
    except Exception:
        return
    if user_id == OWNER_ID:
        return

    cursor.execute("SELECT * FROM adminlocks WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return

    msg  = update.message
    text = msg.text or msg.caption or ""

    checks = [
        ("sticker",  bool(msg.sticker)),
        ("gif",      bool(msg.animation)),
        ("text",     bool(msg.text and not msg.sticker and not msg.animation)),
        ("url",      bool(re.search(r"https?://|t\.me/", text))),
        ("photo",    bool(msg.photo)),
        ("video",    bool(msg.video)),
        ("document", bool(msg.document and not msg.animation)),
        ("audio",    bool(msg.audio)),
        ("voice",    bool(msg.voice)),
    ]

    for idx, (lock_type, matched) in enumerate(checks, start=1):
        if row[idx] and matched:
            try:
                await msg.delete()
            except BadRequest:
                pass
            break


async def enforce_locks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enforce regular locks — only applies to non-admins."""
    if not update.message or update.effective_chat.type == "private":
        return
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if await is_admin(update, context, user_id):
        return
    if await is_approved(chat_id, user_id):
        return
    if await is_whitelisted(chat_id, user_id):
        return

    cursor.execute("SELECT * FROM locks WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return

    msg  = update.message
    text = msg.text or msg.caption or ""

    def has_rtl(s):
        return bool(re.search(
            r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]', s
        ))

    checks = [
        ("sticker",   bool(msg.sticker)),
        ("link",      bool(re.search(r"https?://|t\.me/", text))),
        ("forward",   bool(msg.forward_date or msg.forward_from or msg.forward_from_chat)),
        ("photo",     bool(msg.photo)),
        ("video",     bool(msg.video)),
        ("document",  bool(msg.document and not msg.animation)),
        ("audio",     bool(msg.audio)),
        ("voice",     bool(msg.voice)),
        ("gif",       bool(msg.animation)),
        ("poll",      bool(msg.poll)),
        ("contact",   bool(msg.contact)),
        ("location",  bool(msg.location or msg.venue)),
        ("game",      bool(msg.game)),
        ("inline",    bool(msg.via_bot)),
        ("rtl",       has_rtl(text)),
        ("button",    bool(msg.reply_markup)),
        ("all_media", bool(msg.photo or msg.video or msg.document
                           or msg.audio or msg.voice or msg.animation or msg.sticker)),
        ("text",      bool(msg.text and not any([msg.sticker, msg.animation, msg.photo,
                                                  msg.video, msg.document, msg.audio,
                                                  msg.voice, msg.poll]))),
    ]

    for idx, (lock_type, matched) in enumerate(checks, start=1):
        if row[idx] and matched:
            try:
                await msg.delete()
                replies = [
                    f"🔒 <b>{lock_type.capitalize()}</b> is locked here!",
                    f"⛔ {lock_type.capitalize()} messages are not allowed!",
                    f"🚫 That type is restricted here: <b>{lock_type}</b>",
                    f"📵 {lock_type.capitalize()} locked. Message removed.",
                ]
                notice = await context.bot.send_message(
                    chat_id, random.choice(replies), parse_mode=ParseMode.HTML
                )
                await asyncio.sleep(4)
                try:
                    await notice.delete()
                except BadRequest:
                    pass
            except BadRequest:
                pass
            break


# ══════════════════════════════════════════════════════════
#  FLOOD CONTROL
# ══════════════════════════════════════════════════════════
@admin_only
async def set_flood_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text(
            "Usage: <code>/setflood &lt;msgs&gt; [time]</code>\n"
            "Examples: <code>/setflood 5 10s</code> · <code>/setflood 8 1m</code> · <code>/setflood 0</code>",
            parse_mode=ParseMode.HTML,
        )
    n       = int(context.args[0])
    chat_id = update.effective_chat.id
    window  = 5
    if len(context.args) >= 2:
        parsed = parse_time(context.args[1])
        if parsed:
            window = parsed
        else:
            return await update.message.reply_text(
                "❌ Invalid time! Use <code>10s</code>, <code>1m</code>, etc.", parse_mode=ParseMode.HTML
            )
    cursor.execute("INSERT OR IGNORE INTO flood (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE flood SET limit_count=?, time_window=? WHERE chat_id=?", (n, window, chat_id))
    conn.commit()
    if n == 0:
        await update.message.reply_text("✅ Flood control <b>disabled</b>!", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"✅ Flood limit: <b>{n}</b> messages in <b>{fmt_secs(window)}</b>!",
            parse_mode=ParseMode.HTML,
        )


@admin_only
async def set_flood_action_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] not in ("ban", "kick", "mute", "tmute"):
        return await update.message.reply_text(
            "Usage: <code>/setfloodaction &lt;ban|kick|mute|tmute &lt;time&gt;&gt;</code>",
            parse_mode=ParseMode.HTML,
        )
    action    = context.args[0]
    chat_id   = update.effective_chat.id
    tmute_dur = 300
    if action == "tmute":
        if len(context.args) < 2:
            return await update.message.reply_text(
                "❌ Specify duration! E.g. <code>/setfloodaction tmute 10m</code>",
                parse_mode=ParseMode.HTML,
            )
        parsed = parse_time(context.args[1])
        if not parsed:
            return await update.message.reply_text("❌ Invalid time!")
        tmute_dur = parsed
    cursor.execute("INSERT OR IGNORE INTO flood (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE flood SET action=?, tmute_duration=? WHERE chat_id=?", (action, tmute_dur, chat_id))
    conn.commit()
    label = f"temp-mute for {context.args[1]}" if action == "tmute" else action
    await update.message.reply_text(f"✅ Flood action set to <b>{label}</b>!", parse_mode=ParseMode.HTML)


async def flood_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT limit_count, action, time_window FROM flood WHERE chat_id=?", (update.effective_chat.id,))
    row = cursor.fetchone()
    if not row or row[0] == 0:
        return await update.message.reply_text("🌊 Flood control is <b>disabled</b>.", parse_mode=ParseMode.HTML)
    await update.message.reply_text(
        f"🌊 <b>Flood Settings:</b>\n\n"
        f"• Limit: <b>{row[0]}</b> msgs / {fmt_secs(row[2] or 5)}\n"
        f"• Action: <b>{row[1]}</b>",
        parse_mode=ParseMode.HTML,
    )


async def check_flood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.effective_chat.type == "private":
        return
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if await is_admin(update, context, user_id):
        return
    if await is_approved(chat_id, user_id):
        return
    cursor.execute("SELECT limit_count, action, time_window, tmute_duration FROM flood WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or row[0] == 0:
        return
    limit, action, window, tmute_dur = row[0], row[1], (row[2] or 5), (row[3] or 300)
    now = time.time()
    flood_tracker.setdefault(chat_id, {}).setdefault(user_id, [])
    flood_tracker[chat_id][user_id] = [t for t in flood_tracker[chat_id][user_id] if now - t < window]
    flood_tracker[chat_id][user_id].append(now)
    if len(flood_tracker[chat_id][user_id]) >= limit:
        flood_tracker[chat_id][user_id] = []
        men = mention(update.effective_user)
        try:
            if action == "ban":
                await context.bot.ban_chat_member(chat_id, user_id)
                txt = f"🌊 Flood detected! {men} has been <b>banned</b>!"
            elif action == "kick":
                await context.bot.ban_chat_member(chat_id, user_id)
                await context.bot.unban_chat_member(chat_id, user_id)
                txt = f"🌊 Flood detected! {men} has been <b>kicked</b>!"
            elif action == "tmute":
                until = datetime.now() + timedelta(seconds=tmute_dur)
                await context.bot.restrict_chat_member(
                    chat_id, user_id, ChatPermissions(can_send_messages=False), until_date=until
                )
                context.application.job_queue.run_once(
                    _do_unmute, when=tmute_dur,
                    data=(chat_id, user_id, update.effective_user.first_name),
                )
                txt = f"🌊 Flood detected! {men} temp-muted for <b>{fmt_secs(tmute_dur)}</b>!"
            else:
                await context.bot.restrict_chat_member(
                    chat_id, user_id, ChatPermissions(can_send_messages=False)
                )
                txt = f"🌊 Flood detected! {men} has been <b>muted</b>!"
            await update.message.reply_text(txt, parse_mode=ParseMode.HTML)
        except (BadRequest, Forbidden):
            pass


# ══════════════════════════════════════════════════════════
#  BLACKLIST
# ══════════════════════════════════════════════════════════
@admin_only
async def add_blacklist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /addblacklist <word>")
    word = " ".join(context.args).lower()
    cursor.execute(
        "INSERT OR IGNORE INTO blacklist (chat_id, word) VALUES (?,?)",
        (update.effective_chat.id, word),
    )
    conn.commit()
    await update.message.reply_text(
        f"🚫 <code>{html.escape(word)}</code> added to blacklist!", parse_mode=ParseMode.HTML
    )


@admin_only
async def rm_blacklist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /rmblacklist <word>")
    word = " ".join(context.args).lower()
    cursor.execute("DELETE FROM blacklist WHERE chat_id=? AND word=?", (update.effective_chat.id, word))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(
            f"✅ <code>{html.escape(word)}</code> removed!", parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            f"❌ <code>{html.escape(word)}</code> not in blacklist!", parse_mode=ParseMode.HTML
        )


async def blacklist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT word FROM blacklist WHERE chat_id=?", (update.effective_chat.id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("✅ Blacklist is empty!")
    lst = "\n".join(f"• <code>{html.escape(r[0])}</code>" for r in rows)
    await update.message.reply_text(
        f"🚫 <b>Blacklisted Words ({len(rows)}):</b>\n\n{lst}", parse_mode=ParseMode.HTML
    )


@admin_only
async def blacklistmode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    valid = ("del", "warn", "mute", "ban", "kick")
    if not context.args or context.args[0] not in valid:
        return await update.message.reply_text(f"Usage: /blacklistmode <{'|'.join(valid)}>")
    mode    = context.args[0]
    chat_id = update.effective_chat.id
    cursor.execute("UPDATE blacklist SET action=? WHERE chat_id=?", (mode, chat_id))
    conn.commit()
    await update.message.reply_text(
        f"✅ Blacklist action set to <b>{mode}</b>!", parse_mode=ParseMode.HTML
    )


async def check_blacklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if update.effective_chat.type == "private":
        return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if await is_admin(update, context, user_id):
        return
    if await is_approved(chat_id, user_id):
        return
    text = (update.message.text or update.message.caption or "").lower()
    if not text:
        return
    cursor.execute("SELECT word, action FROM blacklist WHERE chat_id=?", (chat_id,))
    for word, bl_action in cursor.fetchall():
        if word in text:
            action = bl_action or "del"
            try:
                await update.message.delete()
            except BadRequest:
                pass
            men = mention(update.effective_user)
            if action == "warn":
                await _warn_helper(context, chat_id, update.effective_user, f"Blacklisted word: {word}")
            elif action == "mute":
                try:
                    await context.bot.restrict_chat_member(
                        chat_id, user_id, ChatPermissions(can_send_messages=False)
                    )
                    await context.bot.send_message(
                        chat_id,
                        f"🔇 {men} muted for blacklisted word: <code>{html.escape(word)}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                except BadRequest:
                    pass
            elif action == "ban":
                try:
                    await context.bot.ban_chat_member(chat_id, user_id)
                    await context.bot.send_message(
                        chat_id,
                        f"🔨 {men} banned for blacklisted word: <code>{html.escape(word)}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                except BadRequest:
                    pass
            elif action == "kick":
                try:
                    await context.bot.ban_chat_member(chat_id, user_id)
                    await context.bot.unban_chat_member(chat_id, user_id)
                    await context.bot.send_message(
                        chat_id,
                        f"👢 {men} kicked for blacklisted word: <code>{html.escape(word)}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                except BadRequest:
                    pass
            else:
                notice = await context.bot.send_message(
                    chat_id, f"⚠️ {men}, that word is blacklisted here!", parse_mode=ParseMode.HTML
                )
                await asyncio.sleep(4)
                try:
                    await notice.delete()
                except BadRequest:
                    pass
            break


async def _warn_helper(context, chat_id, user, reason):
    cursor.execute("SELECT max_warns, warn_action FROM settings WHERE chat_id=?", (chat_id,))
    s = cursor.fetchone()
    max_w, action = (s[0], s[1]) if s else (3, "ban")
    cursor.execute("SELECT count, reasons FROM warns WHERE chat_id=? AND user_id=?", (chat_id, user.id))
    row   = cursor.fetchone()
    count = (row[0] + 1) if row else 1
    reasons = (row[1] + f"\n{count}. {reason}") if (row and row[1]) else f"1. {reason}"
    cursor.execute(
        "INSERT OR REPLACE INTO warns (chat_id, user_id, count, reasons) VALUES (?,?,?,?)",
        (chat_id, user.id, count, reasons),
    )
    conn.commit()
    msg = random.choice(WARN_MSGS).format(
        user=html.escape(user.first_name), count=count, max=max_w, remaining=max(0, max_w - count)
    )
    if count >= max_w:
        cursor.execute("DELETE FROM warns WHERE chat_id=? AND user_id=?", (chat_id, user.id))
        conn.commit()
        try:
            if action == "ban":
                await context.bot.ban_chat_member(chat_id, user.id)
            elif action == "kick":
                await context.bot.ban_chat_member(chat_id, user.id)
                await context.bot.unban_chat_member(chat_id, user.id)
            else:
                await context.bot.restrict_chat_member(
                    chat_id, user.id, ChatPermissions(can_send_messages=False)
                )
        except BadRequest:
            pass
    await context.bot.send_message(
        chat_id, f"{msg}\n👤 {mention(user)}", parse_mode=ParseMode.HTML
    )


# ══════════════════════════════════════════════════════════
#  AFK
# ══════════════════════════════════════════════════════════
async def afk_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user   = update.effective_user
    reason = " ".join(context.args) if context.args else "AFK"
    cursor.execute(
        "INSERT OR REPLACE INTO afk (user_id, reason, set_time) VALUES (?,?,?)",
        (user.id, reason, int(time.time())),
    )
    conn.commit()
    replies = [
        f"💤 <b>{html.escape(user.first_name)}</b> is now AFK: <i>{html.escape(reason)}</i>",
        f"😴 <b>{html.escape(user.first_name)}</b> went AFK — <i>{html.escape(reason)}</i>",
        f"🌙 <b>{html.escape(user.first_name)}</b> is away: <i>{html.escape(reason)}</i>",
        f"🔕 <b>{html.escape(user.first_name)}</b> has left the chat mentally — <i>{html.escape(reason)}</i>",
        f"🛸 <b>{html.escape(user.first_name)}</b> has ascended to the AFK dimension: <i>{html.escape(reason)}</i>",
    ]
    await update.message.reply_text(random.choice(replies), parse_mode=ParseMode.HTML)


async def check_afk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    user = update.effective_user
    cursor.execute("SELECT reason, set_time FROM afk WHERE user_id=?", (user.id,))
    afk_row = cursor.fetchone()
    if afk_row:
        reason, afk_start = afk_row
        elapsed = int(time.time()) - afk_start
        gone_str = fmt_secs(elapsed)
        cursor.execute("DELETE FROM afk WHERE user_id=?", (user.id,))
        conn.commit()
        replies = [
            f"👋 Welcome back, <b>{html.escape(user.first_name)}</b>! You were AFK for <b>{gone_str}</b>.",
            f"✅ <b>{html.escape(user.first_name)}</b> has risen from the dead after <b>{gone_str}</b>!",
            f"🎉 <b>{html.escape(user.first_name)}</b> is back after <b>{gone_str}</b>! Did you get lost?",
            f"⚡ <b>{html.escape(user.first_name)}</b> respawned after <b>{gone_str}</b>! Loading personality… done.",
        ]
        await update.message.reply_text(random.choice(replies), parse_mode=ParseMode.HTML)
        return

    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        replied = update.message.reply_to_message.from_user
        cursor.execute("SELECT reason, set_time FROM afk WHERE user_id=?", (replied.id,))
        row = cursor.fetchone()
        if row:
            reason, t = row
            elapsed  = int(time.time()) - t
            time_str = fmt_secs(elapsed)
            await update.message.reply_text(
                f"💤 <b>{html.escape(replied.first_name)}</b> is AFK!\n"
                f"📋 Reason: <i>{html.escape(reason)}</i>\n"
                f"⏱️ Since: {time_str} ago",
                parse_mode=ParseMode.HTML,
            )

    if update.message.text:
        for entity in (update.message.entities or []):
            if entity.type == "mention":
                uname = update.message.text[entity.offset + 1: entity.offset + entity.length]
                row_u = sqlite_find_by_username(uname)
                if row_u:
                    cursor.execute("SELECT reason, set_time FROM afk WHERE user_id=?", (row_u[0],))
                    afk_r = cursor.fetchone()
                    if afk_r:
                        reason, t = afk_r
                        elapsed  = int(time.time()) - t
                        time_str = fmt_secs(elapsed)
                        await update.message.reply_text(
                            f"💤 <b>@{uname}</b> is AFK!\n"
                            f"📋 Reason: <i>{html.escape(reason)}</i>\n"
                            f"⏱️ Since: {time_str} ago",
                            parse_mode=ParseMode.HTML,
                        )
                        break


# ══════════════════════════════════════════════════════════
#  @all  TAG-ALL FEATURE
# ══════════════════════════════════════════════════════════
async def _tag_all_worker(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    members: list,
    message_text: str,
    admin_name: str,
):
    """Tag members 10 at a time. Stoppable via _tag_all_tasks[chat_id]['cancelled']."""
    batch_size  = 10
    emoji_pool  = TAG_EMOJIS.copy()
    random.shuffle(emoji_pool)
    emoji_cycle = emoji_pool * ((len(members) // len(emoji_pool)) + 2)

    total   = len(members)
    batches = [members[i:i + batch_size] for i in range(0, total, batch_size)]

    await context.bot.send_message(
        chat_id,
        f"📣 <b>{html.escape(admin_name)}</b> is tagging <b>{total}</b> member(s) "
        f"in <b>{len(batches)}</b> batch(es)…\n"
        f"<i>Send /cancel or /stoptag to stop.</i>",
        parse_mode=ParseMode.HTML,
    )

    for b_idx, batch in enumerate(batches):
        if _tag_all_tasks.get(chat_id, {}).get("cancelled"):
            await context.bot.send_message(
                chat_id, "🛑 Tagging stopped by admin.", parse_mode=ParseMode.HTML
            )
            break

        emoji_offset = b_idx * batch_size
        tags = " ".join(
            f"{emoji_cycle[emoji_offset + i]}{mention(m)}"
            for i, m in enumerate(batch)
        )

        text = f"📣 <b>{html.escape(message_text)}</b>\n\n{tags}"
        try:
            await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)
        except Exception:
            pass

        await asyncio.sleep(1.2)  # Avoid Telegram rate limits
    else:
        await context.bot.send_message(
            chat_id,
            f"✅ Done! All <b>{total}</b> members tagged.",
            parse_mode=ParseMode.HTML,
        )

    # Cleanup
    _tag_all_tasks.pop(chat_id, None)


async def handle_tag_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Triggered when any admin sends a message containing '@all'."""
    if not update.message or update.effective_chat.type == "private":
        return
    if not update.effective_user:
        return

    msg  = update.message
    text = msg.text or msg.caption or ""

    if "@all" not in text.lower():
        return

    if not await is_admin(update, context):
        return await msg.reply_text(
            random.choice(NO_PERM_MSGS), parse_mode=ParseMode.HTML
        )

    chat_id = update.effective_chat.id

    # Prevent concurrent tag-all in same group
    if chat_id in _tag_all_tasks:
        return await msg.reply_text(
            "⚠️ A tag-all is already running! Use /cancel or /stoptag to stop it first."
        )

    # What message to broadcast?
    if msg.reply_to_message:
        broadcast_text = (
            msg.reply_to_message.text
            or msg.reply_to_message.caption
            or "📣 Attention!"
        )
    else:
        broadcast_text = text.replace("@all", "").strip() or "📣 Attention!"

    # Collect admins to exclude from tagging
    try:
        admins = {a.user.id async for a in await update.effective_chat.get_administrators()}
    except Exception:
        admins = set()

    # Always exclude the bot itself and the sender
    admins.add(context.bot.id)
    admins.add(update.effective_user.id)

    # Query chat_members for THIS specific group
    cursor.execute(
        "SELECT user_id, first_name FROM chat_members WHERE chat_id=? ORDER BY last_seen DESC",
        (chat_id,),
    )
    rows = cursor.fetchall()
    members = [_CachedUser(r[0], r[1]) for r in rows if r[0] not in admins]

    total_cached = len(rows)

    if not members:
        # Give a helpful message explaining what they need to do
        cursor.execute("SELECT COUNT(*) FROM chat_members")
        global_total = cursor.fetchone()[0]
        return await msg.reply_text(
            f"⚠️ No members cached for <b>this group</b> yet!\n\n"
            f"Members are recorded the first time they send a message while the bot is active.\n"
            f"<i>Global cache has {global_total} users across all groups — "
            f"but none tagged to this chat yet.</i>\n\n"
            f"💡 Ask members to send any message and the bot will cache them.",
            parse_mode=ParseMode.HTML,
        )

    # Launch background task
    _tag_all_tasks[chat_id] = {"cancelled": False}

    async def _run():
        await _tag_all_worker(context, chat_id, members, broadcast_text, update.effective_user.first_name)

    task = asyncio.create_task(_run())
    _tag_all_tasks[chat_id]["task"] = task


@admin_only
async def cancel_tagall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in _tag_all_tasks:
        _tag_all_tasks[chat_id]["cancelled"] = True
        await update.message.reply_text("🛑 Tag-all will stop after the current batch.")
    else:
        await update.message.reply_text("ℹ️ No tag-all is running.")


# ══════════════════════════════════════════════════════════
#  PIN / UNPIN / PURGE / DEL
# ══════════════════════════════════════════════════════════
@admin_only
async def pin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return await update.message.reply_text("❌ Reply to the message you want to pin!")
    loud = not (context.args and context.args[0].lower() in ("silent", "quiet", "s"))
    try:
        await context.bot.pin_chat_message(
            update.effective_chat.id,
            update.message.reply_to_message.message_id,
            disable_notification=not loud,
        )
        await update.message.reply_text("📌 Message pinned!")
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed to pin: {e}")


@admin_only
async def unpin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if context.args and context.args[0].lower() == "all":
        try:
            await context.bot.unpin_all_chat_messages(chat_id)
            await update.message.reply_text("📌 All messages unpinned!")
        except BadRequest as e:
            await update.message.reply_text(f"❌ Failed: {e}")
    else:
        try:
            if update.message.reply_to_message:
                await context.bot.unpin_chat_message(
                    chat_id, update.message.reply_to_message.message_id
                )
            else:
                await context.bot.unpin_chat_message(chat_id)
            await update.message.reply_text("📌 Message unpinned!")
        except BadRequest as e:
            await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def purge_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return await update.message.reply_text("❌ Reply to the first message you want purged!")
    start_id = update.message.reply_to_message.message_id
    end_id   = update.message.message_id
    chat_id  = update.effective_chat.id
    deleted  = 0
    for mid in range(start_id, end_id + 1):
        try:
            await context.bot.delete_message(chat_id, mid)
            deleted += 1
        except BadRequest:
            pass
    note = await context.bot.send_message(
        chat_id, f"🗑️ Purged <b>{deleted}</b> messages!", parse_mode=ParseMode.HTML
    )
    await asyncio.sleep(3)
    try:
        await note.delete()
    except BadRequest:
        pass


@admin_only
async def spurge_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return
    start_id = update.message.reply_to_message.message_id
    end_id   = update.message.message_id
    chat_id  = update.effective_chat.id
    try:
        await update.message.delete()
    except BadRequest:
        pass
    for mid in range(start_id, end_id + 1):
        try:
            await context.bot.delete_message(chat_id, mid)
        except BadRequest:
            pass


@admin_only
async def del_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return await update.message.reply_text("❌ Reply to the message you want to delete!")
    try:
        await update.message.reply_to_message.delete()
        await update.message.delete()
    except BadRequest:
        await update.message.reply_text("❌ I can't delete that message!")


# ══════════════════════════════════════════════════════════
#  PROMOTE / DEMOTE / TITLE
# ══════════════════════════════════════════════════════════
@admin_only
async def promote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    try:
        await context.bot.promote_chat_member(
            update.effective_chat.id, target.id,
            can_manage_chat=True, can_delete_messages=True,
            can_restrict_members=True, can_invite_users=True,
            can_pin_messages=True, can_manage_video_chats=True,
        )
        await update.message.reply_text(
            f"⭐ {mention(target)} has been <b>promoted</b> to admin!", parse_mode=ParseMode.HTML
        )
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def demote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    try:
        await context.bot.promote_chat_member(
            update.effective_chat.id, target.id,
            can_manage_chat=False, can_delete_messages=False,
            can_restrict_members=False, can_invite_users=False,
            can_pin_messages=False,
        )
        await update.message.reply_text(
            f"🔻 {mention(target)} has been <b>demoted</b>!", parse_mode=ParseMode.HTML
        )
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def title_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    title_parts = context.args[1:] if not update.message.reply_to_message else context.args
    if not title_parts:
        return await update.message.reply_text("Usage: /title @user <title>")
    title = " ".join(title_parts)[:16]
    try:
        await context.bot.set_chat_administrator_custom_title(
            update.effective_chat.id, target.id, title
        )
        await update.message.reply_text(
            f"🏷️ {mention(target)}'s title set to <b>{html.escape(title)}</b>!",
            parse_mode=ParseMode.HTML,
        )
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed: {e}")


# ══════════════════════════════════════════════════════════
#  INFO / ID / ADMINLIST / WHOIS / REPORT / CHATINFO / STATS
# ══════════════════════════════════════════════════════════
async def info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    elif context.args:
        raw = context.args[0].lstrip("@")
        try:
            target = await context.bot.get_chat(int(raw) if raw.isdigit() else f"@{raw}")
        except (BadRequest, ValueError):
            row = sqlite_find_by_username(raw) if not raw.isdigit() else sqlite_find_by_id(int(raw))
            if row:
                target = _CachedUser(*row)
            else:
                # Try MongoDB
                doc = (mongo_find_by_username(raw) if not raw.isdigit() else mongo_find_by_id(int(raw)))
                if doc:
                    target = _CachedUser(_mongo_uid(doc), _mongo_fname(doc), doc.get("last_name"), _mongo_uname(doc))
                else:
                    return await update.message.reply_text("❌ User not found!")
    else:
        target = update.effective_user
    uname = f"@{target.username}" if getattr(target, "username", None) else "—"
    ln    = getattr(target, "last_name", None) or "—"
    await update.message.reply_text(
        f"ℹ️ <b>User Info:</b>\n\n"
        f"👤 Name: {html.escape(target.first_name or '')}\n"
        f"📝 Last name: {html.escape(ln)}\n"
        f"🆔 ID: <code>{target.id}</code>\n"
        f"📛 Username: {uname}\n"
        f"🔗 Mention: {mention(target)}",
        parse_mode=ParseMode.HTML,
    )


async def whois_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    elif context.args:
        raw = context.args[0].lstrip("@")
        try:
            target = await context.bot.get_chat(int(raw) if raw.isdigit() else f"@{raw}")
        except (BadRequest, ValueError):
            row = sqlite_find_by_username(raw) if not raw.isdigit() else sqlite_find_by_id(int(raw))
            if row:
                target = _CachedUser(*row)
            else:
                doc = (mongo_find_by_username(raw) if not raw.isdigit() else mongo_find_by_id(int(raw)))
                if doc:
                    target = _CachedUser(_mongo_uid(doc), _mongo_fname(doc), doc.get("last_name"), _mongo_uname(doc))
                else:
                    return await update.message.reply_text("❌ User not found!")
    else:
        target = update.effective_user

    uname   = f"@{target.username}" if getattr(target, "username", None) else "—"
    chat_id = update.effective_chat.id
    gban_doc = is_gbanned(target.id)
    cursor.execute("SELECT count FROM warns WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    warns_row = cursor.fetchone()
    cursor.execute("SELECT 1 FROM approved WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    appr = "✅ Yes" if cursor.fetchone() else "❌ No"
    cursor.execute("SELECT 1 FROM whitelist WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    wl = "✅ Yes" if cursor.fetchone() else "❌ No"
    is_adm, adm_title = False, ""
    try:
        cm = await update.effective_chat.get_member(target.id)
        is_adm = cm.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
        if hasattr(cm, "custom_title") and cm.custom_title:
            adm_title = f" ({html.escape(cm.custom_title)})"
    except Exception:
        pass
    await update.message.reply_text(
        f"🔎 <b>Who Is {mention(target)}?</b>\n\n"
        f"🆔 ID: <code>{target.id}</code>\n"
        f"📛 Username: {uname}\n"
        f"👤 Admin: {'✅' + adm_title if is_adm else '❌'}\n"
        f"⚠️ Warns here: {warns_row[0] if warns_row else 0}\n"
        f"✅ Approved: {appr}\n"
        f"🛡️ Whitelisted: {wl}\n"
        f"🌍 Globally banned: "
        f"{'⚠️ YES — ' + html.escape(gban_doc.get('reason','?')) if gban_doc else '✅ No'}",
        parse_mode=ParseMode.HTML,
    )


async def chatinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat  = update.effective_chat
    count = await chat.get_member_count()
    uname = f"@{chat.username}" if chat.username else "—"
    cursor.execute("SELECT limit_count FROM flood WHERE chat_id=?", (chat.id,))
    fl = cursor.fetchone()
    cursor.execute("SELECT welcome_enabled FROM welcome WHERE chat_id=?", (chat.id,))
    wl = cursor.fetchone()
    await update.message.reply_text(
        f"ℹ️ <b>Chat Info:</b>\n\n"
        f"💬 Title: {html.escape(chat.title or '')}\n"
        f"🆔 ID: <code>{chat.id}</code>\n"
        f"👥 Members: {count}\n"
        f"📋 Type: {chat.type.capitalize()}\n"
        f"🔗 Username: {uname}\n"
        f"👋 Welcome: {'✅' if (wl and wl[0]) else '❌'}\n"
        f"🌊 Flood control: {'✅ (limit ' + str(fl[0]) + ')' if (fl and fl[0]) else '❌'}",
        parse_mode=ParseMode.HTML,
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        u = update.message.reply_to_message.from_user
        await update.message.reply_text(
            f"🆔 User: <code>{u.id}</code>\n💬 Chat: <code>{update.effective_chat.id}</code>",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"🆔 Your ID: <code>{update.effective_user.id}</code>\n"
            f"💬 Chat ID: <code>{update.effective_chat.id}</code>",
            parse_mode=ParseMode.HTML,
        )


async def adminlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admins = await update.effective_chat.get_administrators()
    lines  = []
    for adm in admins:
        if adm.user.is_bot:
            continue
        tag  = "👑" if adm.status == ChatMemberStatus.OWNER else "⭐"
        nm   = html.escape(adm.user.first_name or "")
        un   = f" (@{adm.user.username})" if adm.user.username else ""
        titl = f" | <i>{html.escape(adm.custom_title)}</i>" if hasattr(adm, "custom_title") and adm.custom_title else ""
        lines.append(f"{tag} {nm}{un}{titl}")
    await update.message.reply_text(
        f"👑 <b>Admin List</b> ({len(lines)}):\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return await update.message.reply_text("❌ Reply to the message you want to report!")
    reporter = update.effective_user
    reported = update.message.reply_to_message.from_user
    reason   = " ".join(context.args) if context.args else "No reason provided"
    admins   = await update.effective_chat.get_administrators()
    admin_mentions = " ".join(
        f'<a href="tg://user?id={a.user.id}">{html.escape(a.user.first_name)}</a>'
        for a in admins if not a.user.is_bot
    )
    await update.message.reply_text(
        f"🚨 <b>Report Filed!</b>\n\n"
        f"👤 Reporter: {mention(reporter)}\n"
        f"🚫 Reported: {mention(reported)}\n"
        f"📋 Reason: <i>{html.escape(reason)}</i>\n\n"
        f"📢 Notified: {admin_mentions}",
        parse_mode=ParseMode.HTML,
    )


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - _bot_start_time)
    bans   = get_stat("bans")
    kicks  = get_stat("kicks")
    mutes  = get_stat("mutes")
    warns  = get_stat("warns")
    cursor.execute("SELECT COUNT(*) FROM user_cache")
    total_users = cursor.fetchone()[0]
    if mongo_gbans_col:
        try:
            gban_count = mongo_gbans_col.count_documents({})
        except Exception:
            gban_count = 0
    else:
        gban_count = 0
    await update.message.reply_text(
        f"📊 <b>Bot Statistics</b>\n\n"
        f"⏱️ Uptime: <b>{fmt_secs(uptime)}</b>\n"
        f"👥 Known users: <b>{total_users}</b>\n"
        f"🌍 Global bans: <b>{gban_count}</b>\n\n"
        f"<b>Actions this session:</b>\n"
        f"🔨 Bans: {bans}\n"
        f"👢 Kicks: {kicks}\n"
        f"🔇 Mutes: {mutes}\n"
        f"⚠️ Warns: {warns}",
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  BOTSTATS & BOTGROUPS  (owner only)
# ══════════════════════════════════════════════════════════
@owner_only
async def botstats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - _bot_start_time)

    cursor.execute("SELECT COUNT(*) FROM user_cache")
    sqlite_users = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM dm_users")
    dm_count = cursor.fetchone()[0]

    # Group count: prefer MongoDB (persists across redeploys)
    if mongo_groups_col is not None:
        try:
            group_count = mongo_groups_col.count_documents({})
        except Exception:
            cursor.execute("SELECT COUNT(*) FROM bot_groups")
            group_count = cursor.fetchone()[0]
    else:
        cursor.execute("SELECT COUNT(*) FROM bot_groups")
        group_count = cursor.fetchone()[0]

    if mongo_users_col:
        try:
            mongo_user_count = mongo_users_col.count_documents({})
        except Exception:
            mongo_user_count = 0
    else:
        mongo_user_count = 0

    if mongo_gbans_col:
        try:
            gban_count = mongo_gbans_col.count_documents({})
        except Exception:
            gban_count = 0
    else:
        gban_count = 0

    # Stats: get_stat() now reads from MongoDB first (persistent)
    bans  = get_stat("bans")
    kicks = get_stat("kicks")
    mutes = get_stat("mutes")
    warns = get_stat("warns")

    mongo_status = "✅ Connected" if mongo_users_col is not None else "❌ Not connected"

    await update.message.reply_text(
        f"🤖 <b>Bot Stats (Owner View)</b>\n\n"
        f"⏱️ Uptime: <b>{fmt_secs(uptime)}</b>\n"
        f"🗄️ MongoDB: <b>{mongo_status}</b>\n\n"
        f"👥 <b>User Data:</b>\n"
        f"• SQLite cache: <b>{sqlite_users}</b> users\n"
        f"• MongoDB: <b>{mongo_user_count}</b> users\n"
        f"• Started DM: <b>{dm_count}</b> users\n\n"
        f"🏘️ <b>Groups (all-time):</b> <b>{group_count}</b>\n\n"
        f"🌍 <b>Global Bans:</b> <b>{gban_count}</b>\n\n"
        f"📊 <b>All-time Actions (persistent):</b>\n"
        f"🔨 Bans: <b>{bans}</b> | 👢 Kicks: <b>{kicks}</b>\n"
        f"🔇 Mutes: <b>{mutes}</b> | ⚠️ Warns: <b>{warns}</b>",
        parse_mode=ParseMode.HTML,
    )


@owner_only
async def cache_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show users cached for this specific group (group-scoped from MongoDB + SQLite)."""
    chat = update.effective_chat
    is_group = chat.type not in ("private",)

    if not is_group:
        # In PM: show global count summary
        cursor.execute("SELECT COUNT(*) FROM user_cache")
        sqlite_total = cursor.fetchone()[0]
        mongo_total = 0
        if mongo_users_col:
            try:
                mongo_total = mongo_users_col.count_documents({})
            except Exception:
                pass
        return await update.message.reply_text(
            f"📊 <b>Global User Cache</b>\n\n"
            f"• SQLite: <b>{sqlite_total}</b> users\n"
            f"• MongoDB: <b>{mongo_total}</b> users\n\n"
            f"<i>Run in a group to see group-specific members.</i>",
            parse_mode=ParseMode.HTML,
        )

    chat_id = chat.id
    users: list[tuple[int, str, str | None]] = []  # (uid, fname, username)

    # ── 1. Try MongoDB first (most persistent) ──────────────────────────
    if mongo_users_col:
        try:
            docs = list(mongo_users_col.find(
                {"seen_in_chats": chat_id},
                {"user_id": 1, "first_name": 1, "username": 1},
            ).limit(500))
            for d in docs:
                uid = _mongo_uid(d)
                if uid:
                    users.append((uid, _mongo_fname(d), d.get("username")))
        except Exception:
            pass

    # ── 2. Fall back / supplement with SQLite chat_members ───────────────
    seen_ids = {u[0] for u in users}
    cursor.execute(
        "SELECT user_id, first_name, username FROM chat_members WHERE chat_id=? ORDER BY last_seen DESC",
        (chat_id,),
    )
    for uid, fname, uname in cursor.fetchall():
        if uid not in seen_ids:
            users.append((uid, fname or "Unknown", uname))
            seen_ids.add(uid)

    if not users:
        return await update.message.reply_text(
            "📭 No users cached for this group yet.\n"
            "Members are cached as they send messages while the bot is active.",
            parse_mode=ParseMode.HTML,
        )

    users.sort(key=lambda x: x[1].lower())
    total = len(users)
    chunk_size = 80
    for i in range(0, total, chunk_size):
        chunk = users[i:i + chunk_size]
        lines = []
        for j, (uid, fname, uname) in enumerate(chunk):
            un = f" (@{uname})" if uname else ""
            lines.append(f"{i + j + 1}. <a href='tg://user?id={uid}'>{html.escape(fname)}</a>{un}")
        header = f"👥 <b>Group Members Cache</b> — {html.escape(chat.title or '')}\n<i>{total} users tracked</i>\n\n" if i == 0 else ""
        await update.message.reply_text(
            f"{header}" + "\n".join(lines),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


@owner_only
async def botgroups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = []

    # Primary: MongoDB (persists across redeploys)
    if mongo_groups_col is not None:
        try:
            docs = list(mongo_groups_col.find({}, {"chat_id": 1, "title": 1, "username": 1, "joined_at": 1})
                        .sort("joined_at", -1).limit(50))
            for d in docs:
                rows.append((
                    d.get("chat_id"), d.get("title", ""), d.get("username", ""), d.get("joined_at")
                ))
        except Exception:
            pass

    # Fallback: SQLite (session-only, may have groups not yet in Mongo)
    if not rows:
        cursor.execute("SELECT chat_id, title, username, joined_at FROM bot_groups ORDER BY joined_at DESC LIMIT 50")
        rows = cursor.fetchall()

    if not rows:
        return await update.message.reply_text(
            "❌ No groups tracked yet. The bot auto-tracks groups as members join/message."
        )
    lines = []
    for chat_id, title, username, joined_at in rows:
        un   = f" (@{username})" if username else ""
        date = datetime.fromtimestamp(joined_at).strftime("%Y-%m-%d") if joined_at else "?"
        lines.append(f"• <b>{html.escape(title or '?')}</b>{un}\n  ID: <code>{chat_id}</code> | Joined: {date}")
    await update.message.reply_text(
        f"🏘️ <b>Groups Bot Is In ({len(rows)}):</b>\n\n" + "\n\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  APPROVAL SYSTEM
# ══════════════════════════════════════════════════════════
@admin_only
async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    cursor.execute(
        "INSERT OR IGNORE INTO approved (chat_id, user_id) VALUES (?,?)",
        (update.effective_chat.id, target.id),
    )
    conn.commit()
    await update.message.reply_text(
        f"✅ {mention(target)} is now <b>approved</b>! They bypass locks and blacklist.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def unapprove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    cursor.execute(
        "DELETE FROM approved WHERE chat_id=? AND user_id=?",
        (update.effective_chat.id, target.id),
    )
    conn.commit()
    await update.message.reply_text(
        f"❌ {mention(target)}'s approval has been <b>revoked</b>.", parse_mode=ParseMode.HTML
    )


@admin_only
async def approved_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT user_id FROM approved WHERE chat_id=?", (update.effective_chat.id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📭 No approved users!")
    lines = []
    for (uid,) in rows:
        row = sqlite_find_by_id(uid)
        nm  = html.escape(row[1] if row else str(uid))
        un  = f" (@{row[3]})" if row and row[3] else ""
        lines.append(f"• <code>{uid}</code> — {nm}{un}")
    await update.message.reply_text(
        f"✅ <b>Approved Users ({len(rows)}):</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def unapproveall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("DELETE FROM approved WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("✅ All approvals revoked!")


# ══════════════════════════════════════════════════════════
#  WHITELIST
# ══════════════════════════════════════════════════════════
@admin_only
async def whitelist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    cursor.execute(
        "INSERT OR IGNORE INTO whitelist (chat_id, user_id) VALUES (?,?)",
        (update.effective_chat.id, target.id),
    )
    conn.commit()
    await update.message.reply_text(
        f"🛡️ {mention(target)} is now <b>whitelisted</b>!", parse_mode=ParseMode.HTML
    )


@admin_only
async def unwhitelist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    cursor.execute(
        "DELETE FROM whitelist WHERE chat_id=? AND user_id=?",
        (update.effective_chat.id, target.id),
    )
    conn.commit()
    await update.message.reply_text(
        f"❌ {mention(target)} removed from whitelist.", parse_mode=ParseMode.HTML
    )


@admin_only
async def whitelisted_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT user_id FROM whitelist WHERE chat_id=?", (update.effective_chat.id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📭 No whitelisted users!")
    lines = []
    for (uid,) in rows:
        row = sqlite_find_by_id(uid)
        nm  = html.escape(row[1] if row else str(uid))
        lines.append(f"• <code>{uid}</code> — {nm}")
    await update.message.reply_text(
        f"🛡️ <b>Whitelisted Users ({len(rows)}):</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  DISABLE / ENABLE COMMANDS
# ══════════════════════════════════════════════════════════
DISABLEABLE = [
    "ban", "kick", "mute", "tmute", "tban", "warn", "warns",
    "notes", "filters", "locks", "blacklist", "afk", "info", "rules", "flood",
]


@admin_only
async def disable_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text(
            f"Usage: /disable &lt;command&gt;\nDisableable: <code>{' '.join(DISABLEABLE)}</code>",
            parse_mode=ParseMode.HTML,
        )
    cmd = context.args[0].lower().lstrip("/")
    if cmd not in DISABLEABLE:
        return await update.message.reply_text(
            f"❌ <code>{cmd}</code> can't be disabled!", parse_mode=ParseMode.HTML
        )
    cursor.execute(
        "INSERT OR IGNORE INTO disabled_cmds (chat_id, command) VALUES (?,?)",
        (update.effective_chat.id, cmd),
    )
    conn.commit()
    await update.message.reply_text(f"❌ <code>/{cmd}</code> disabled!", parse_mode=ParseMode.HTML)


@admin_only
async def enable_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /enable <command>")
    cmd = context.args[0].lower().lstrip("/")
    cursor.execute(
        "DELETE FROM disabled_cmds WHERE chat_id=? AND command=?",
        (update.effective_chat.id, cmd),
    )
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"✅ <code>/{cmd}</code> enabled!", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"ℹ️ <code>/{cmd}</code> was not disabled.", parse_mode=ParseMode.HTML)


async def disabled_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT command FROM disabled_cmds WHERE chat_id=?", (update.effective_chat.id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("✅ No commands are disabled!")
    lst = "\n".join(f"• <code>/{r[0]}</code>" for r in rows)
    await update.message.reply_text(f"❌ <b>Disabled Commands:</b>\n\n{lst}", parse_mode=ParseMode.HTML)


# ══════════════════════════════════════════════════════════
#  ANTI-BOT
# ══════════════════════════════════════════════════════════
@admin_only
async def antibot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT antibot FROM settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    new_val = 0 if (row and row[0]) else 1
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE settings SET antibot=? WHERE chat_id=?", (new_val, chat_id))
    conn.commit()
    status = "enabled ✅ (bots will be auto-removed on join)" if new_val else "disabled ❌"
    await update.message.reply_text(f"🤖 Anti-bot: {status}")


# ══════════════════════════════════════════════════════════
#  LOG CHANNEL
# ══════════════════════════════════════════════════════════
@admin_only
async def setlog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
        cursor.execute("UPDATE settings SET log_channel=0 WHERE chat_id=?", (chat_id,))
        conn.commit()
        return await update.message.reply_text("✅ Log channel removed.")
    try:
        log_id = int(context.args[0])
    except ValueError:
        return await update.message.reply_text("❌ Provide a valid channel ID (negative number).")
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE settings SET log_channel=? WHERE chat_id=?", (log_id, chat_id))
    conn.commit()
    await update.message.reply_text(
        f"✅ Log channel set to <code>{log_id}</code>!", parse_mode=ParseMode.HTML
    )


@admin_only
async def unsetlog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE settings SET log_channel=0 WHERE chat_id=?", (chat_id,))
    conn.commit()
    await update.message.reply_text("✅ Log channel has been removed.")


# ══════════════════════════════════════════════════════════
#  INVITE LINK
# ══════════════════════════════════════════════════════════
@admin_only
async def invite_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        link = await context.bot.export_chat_invite_link(update.effective_chat.id)
        await update.message.reply_text(f"🔗 <b>Invite Link:</b>\n{link}", parse_mode=ParseMode.HTML)
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed: {e}")


# ══════════════════════════════════════════════════════════
#  CONNECT / DISCONNECT
# ══════════════════════════════════════════════════════════
async def connect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if update.effective_chat.type != "private":
        chat_id = update.effective_chat.id
        cursor.execute(
            "INSERT OR REPLACE INTO connections (user_id, chat_id) VALUES (?,?)", (user_id, chat_id)
        )
        conn.commit()
        await update.message.reply_text("✅ Connected! You can now use management commands in my PM.")
    else:
        cursor.execute("SELECT chat_id FROM connections WHERE user_id=?", (user_id,))
        row = cursor.fetchone()
        if row:
            await update.message.reply_text(
                f"🔗 Connected to: <code>{row[0]}</code>", parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("❌ Not connected. Use /connect in a group first.")


async def disconnect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("DELETE FROM connections WHERE user_id=?", (update.effective_user.id,))
    conn.commit()
    await update.message.reply_text("✅ Disconnected.")


# ══════════════════════════════════════════════════════════
#  BROADCAST (owner only)
# ══════════════════════════════════════════════════════════
@owner_only
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args and not update.message.reply_to_message:
        return await update.message.reply_text("Usage: /broadcast <message> or reply to a message")
    if update.message.reply_to_message:
        bcast_text = update.message.reply_to_message.text or update.message.reply_to_message.caption
    else:
        bcast_text = " ".join(context.args)
    if not bcast_text:
        return await update.message.reply_text("❌ No text to broadcast!")
    # Use bot_groups for all known groups, fall back to welcome table for older entries
    cursor.execute(
        "SELECT chat_id FROM bot_groups "
        "UNION SELECT DISTINCT chat_id FROM welcome"
    )
    chats = cursor.fetchall()
    sent, failed = 0, 0
    for (cid,) in chats:
        try:
            await context.bot.send_message(
                cid,
                f"📢 <b>Broadcast:</b>\n\n{html.escape(bcast_text)}",
                parse_mode=ParseMode.HTML,
            )
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"✅ Done! Sent: {sent} | Failed: {failed}")


# ══════════════════════════════════════════════════════════
#  FEDERATION SYSTEM
# ══════════════════════════════════════════════════════════
async def newfed_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /newfed <federation name>")
    fed_name = " ".join(context.args)
    user_id  = update.effective_user.id
    fed_id   = str(uuid.uuid4())[:8]
    cursor.execute(
        "INSERT INTO feds (fed_id, fed_name, owner_id) VALUES (?,?,?)",
        (fed_id, fed_name, user_id),
    )
    conn.commit()
    await update.message.reply_text(
        f"🌐 Federation <b>{html.escape(fed_name)}</b> created!\n"
        f"Fed ID: <code>{fed_id}</code>\n\n"
        f"Use <code>/joinfed {fed_id}</code> in groups to add them.",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def joinfed_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /joinfed <fed_id>")
    fed_id  = context.args[0]
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_name, owner_id, chats FROM feds WHERE fed_id=?", (fed_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("❌ Federation not found!")
    fed_name, owner_id, chats_json = row
    if update.effective_user.id != owner_id and update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("❌ Only the federation owner can add groups!")
    chats = json.loads(chats_json or "[]")
    if chat_id not in chats:
        chats.append(chat_id)
    cursor.execute("UPDATE feds SET chats=? WHERE fed_id=?", (json.dumps(chats), fed_id))
    conn.commit()
    await update.message.reply_text(
        f"✅ This group joined federation <b>{html.escape(fed_name)}</b>!",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def leavefed_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_id, fed_name, chats FROM feds")
    for fed_id, fed_name, chats_json in cursor.fetchall():
        chats = json.loads(chats_json or "[]")
        if chat_id in chats:
            chats.remove(chat_id)
            cursor.execute("UPDATE feds SET chats=? WHERE fed_id=?", (json.dumps(chats), fed_id))
            conn.commit()
            return await update.message.reply_text(
                f"✅ Left federation <b>{html.escape(fed_name)}</b>!", parse_mode=ParseMode.HTML
            )
    await update.message.reply_text("❌ This chat is not in any federation!")


async def fedinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_id, fed_name, owner_id, admins, chats FROM feds")
    for fed_id, fed_name, owner_id, admins_json, chats_json in cursor.fetchall():
        chats = json.loads(chats_json or "[]")
        if chat_id in chats:
            admins = json.loads(admins_json or "[]")
            cursor.execute("SELECT COUNT(*) FROM fedbans WHERE fed_id=?", (fed_id,))
            ban_count = cursor.fetchone()[0]
            await update.message.reply_text(
                f"🌐 <b>Federation Info:</b>\n\n"
                f"Name: <b>{html.escape(fed_name)}</b>\n"
                f"ID: <code>{fed_id}</code>\n"
                f"Owner: <code>{owner_id}</code>\n"
                f"Groups: {len(chats)}\n"
                f"Admins: {len(admins)}\n"
                f"Fed bans: {ban_count}",
                parse_mode=ParseMode.HTML,
            )
            return
    await update.message.reply_text("❌ This chat is not in any federation!")


async def fban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    cursor.execute("SELECT fed_id, fed_name, owner_id, admins, chats FROM feds")
    fed_row = None
    for r in cursor.fetchall():
        chats = json.loads(r[4] or "[]")
        if chat_id in chats:
            fed_row = r
            break
    if not fed_row:
        return await update.message.reply_text("❌ This chat is not in any federation!")
    fed_id, fed_name, owner_id, admins_json, chats_json = fed_row
    admins = json.loads(admins_json or "[]")
    if user_id != owner_id and user_id not in admins and user_id != OWNER_ID:
        return await update.message.reply_text("❌ Only the federation owner/admins can fban!")
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    reason = " ".join(context.args[1:]) if (context.args and not update.message.reply_to_message) else (" ".join(context.args) if context.args else "No reason")
    cursor.execute(
        "INSERT OR REPLACE INTO fedbans (fed_id, user_id, reason) VALUES (?,?,?)",
        (fed_id, target.id, reason),
    )
    conn.commit()
    chats   = json.loads(chats_json or "[]")
    success = 0
    for cid in chats:
        try:
            await context.bot.ban_chat_member(cid, target.id)
            success += 1
        except Exception:
            pass
    await update.message.reply_text(
        f"⚡ <b>Federation Ban!</b>\n\n"
        f"User: {mention(target)}\n"
        f"Federation: <b>{html.escape(fed_name)}</b>\n"
        f"Reason: <i>{html.escape(reason)}</i>\n"
        f"Banned from {success}/{len(chats)} groups.",
        parse_mode=ParseMode.HTML,
    )


async def unfban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    cursor.execute("SELECT fed_id, fed_name, owner_id, admins, chats FROM feds")
    fed_row = None
    for r in cursor.fetchall():
        chats = json.loads(r[4] or "[]")
        if chat_id in chats:
            fed_row = r
            break
    if not fed_row:
        return await update.message.reply_text("❌ This chat is not in any federation!")
    fed_id, fed_name, owner_id, admins_json, chats_json = fed_row
    admins = json.loads(admins_json or "[]")
    if user_id != owner_id and user_id not in admins and user_id != OWNER_ID:
        return await update.message.reply_text("❌ Only the federation owner/admins can unfban!")
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    cursor.execute("DELETE FROM fedbans WHERE fed_id=? AND user_id=?", (fed_id, target.id))
    conn.commit()
    for cid in json.loads(chats_json or "[]"):
        try:
            await context.bot.unban_chat_member(cid, target.id)
        except Exception:
            pass
    await update.message.reply_text(
        f"✅ {mention(target)} un-federation-banned from <b>{html.escape(fed_name)}</b>!",
        parse_mode=ParseMode.HTML,
    )


async def fedadmins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_id, fed_name, owner_id, admins, chats FROM feds")
    for fed_id, fed_name, owner_id, admins_json, chats_json in cursor.fetchall():
        chats = json.loads(chats_json or "[]")
        if chat_id not in chats:
            continue
        admins = json.loads(admins_json or "[]")
        lines  = [f"👑 Owner: <code>{owner_id}</code>"]
        for adm_id in admins:
            lines.append(f"⭐ <code>{adm_id}</code>")
        await update.message.reply_text(
            f"🌐 <b>Fed Admins for {html.escape(fed_name)}:</b>\n\n" + "\n".join(lines),
            parse_mode=ParseMode.HTML,
        )
        return
    await update.message.reply_text("❌ This chat is not in any federation!")


# ══════════════════════════════════════════════════════════
#  NEW SQLITE TABLES (initialised in init_db via patch)
# ══════════════════════════════════════════════════════════
# These are created at startup via _init_extra_tables()
def _init_extra_tables():
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS captcha_settings (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            mode TEXT DEFAULT 'button',
            timeout INTEGER DEFAULT 120
        );
        CREATE TABLE IF NOT EXISTS captcha_pending (
            chat_id INTEGER, user_id INTEGER, message_id INTEGER,
            expires_at INTEGER,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS force_sub (
            chat_id INTEGER PRIMARY KEY,
            channel_id INTEGER DEFAULT 0,
            channel_username TEXT DEFAULT '',
            enabled INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS karma (
            chat_id INTEGER, user_id INTEGER, points INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS custom_cmds (
            chat_id INTEGER, cmd TEXT, response TEXT,
            PRIMARY KEY (chat_id, cmd)
        );
        CREATE TABLE IF NOT EXISTS group_msg_count (
            chat_id INTEGER, user_id INTEGER, count INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS anti_raid (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            threshold INTEGER DEFAULT 5,
            window INTEGER DEFAULT 10,
            action TEXT DEFAULT 'kick'
        );
        CREATE TABLE IF NOT EXISTS slowmode (
            chat_id INTEGER PRIMARY KEY,
            seconds INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS slowmode_tracker (
            chat_id INTEGER, user_id INTEGER, last_msg INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS caps_filter (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            min_length INTEGER DEFAULT 10,
            percent INTEGER DEFAULT 70
        );
        CREATE TABLE IF NOT EXISTS emoji_filter (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            max_count INTEGER DEFAULT 5
        );
        CREATE TABLE IF NOT EXISTS link_whitelist (
            chat_id INTEGER, domain TEXT,
            PRIMARY KEY (chat_id, domain)
        );
        CREATE TABLE IF NOT EXISTS anti_forward (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS quotes (
            chat_id INTEGER, quote_id INTEGER,
            user_id INTEGER, user_name TEXT,
            content TEXT, added_at INTEGER,
            PRIMARY KEY (chat_id, quote_id)
        );
        CREATE TABLE IF NOT EXISTS reminders (
            reminder_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER, user_id INTEGER,
            fire_at INTEGER, text TEXT, done INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS scheduled_msgs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER, text TEXT,
            fire_at INTEGER, repeat_secs INTEGER DEFAULT 0,
            done INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS auto_delete (
            chat_id INTEGER PRIMARY KEY,
            seconds INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS chat_backup (
            chat_id INTEGER PRIMARY KEY,
            data TEXT DEFAULT '{}',
            backed_up_at INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS trivia_scores (
            chat_id INTEGER, user_id INTEGER, score INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS ghost_mode (
            chat_id INTEGER, user_id INTEGER,
            PRIMARY KEY (chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS report_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER, reporter_id INTEGER, reported_id INTEGER,
            reason TEXT, reported_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS user_notes (
            owner_id INTEGER, note_key TEXT, content TEXT,
            PRIMARY KEY (owner_id, note_key)
        );
    """)
    conn.commit()


# ══════════════════════════════════════════════════════════
#  CAPTCHA / NEW-MEMBER VERIFICATION
# ══════════════════════════════════════════════════════════
_captcha_pending: dict[tuple, int] = {}   # (chat_id, user_id) -> message_id


@admin_only
async def captcha_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle captcha / configure: /captcha on|off|status|button|math [timeout]"""
    chat_id = update.effective_chat.id
    args = context.args or []
    if not args:
        cursor.execute("SELECT enabled, mode, timeout FROM captcha_settings WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        if row:
            en, mode, t = row
        else:
            en, mode, t = 0, "button", 120
        return await update.message.reply_text(
            f"🔒 <b>Captcha Settings</b>\n\n"
            f"Status: {'✅ Enabled' if en else '❌ Disabled'}\n"
            f"Mode: <b>{mode}</b>\n"
            f"Timeout: <b>{t}s</b>\n\n"
            f"Usage:\n"
            f"/captcha on|off — enable / disable\n"
            f"/captcha button — button-click challenge\n"
            f"/captcha math — simple math challenge\n"
            f"/captcha timeout 120 — set timeout in seconds",
            parse_mode=ParseMode.HTML,
        )
    sub = args[0].lower()
    cursor.execute("INSERT OR IGNORE INTO captcha_settings (chat_id) VALUES (?)", (chat_id,))
    if sub == "on":
        cursor.execute("UPDATE captcha_settings SET enabled=1 WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text("✅ Captcha <b>enabled</b>! New members will be asked to verify.", parse_mode=ParseMode.HTML)
    elif sub == "off":
        cursor.execute("UPDATE captcha_settings SET enabled=0 WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text("❌ Captcha <b>disabled</b>.", parse_mode=ParseMode.HTML)
    elif sub in ("button", "math"):
        cursor.execute("UPDATE captcha_settings SET mode=? WHERE chat_id=?", (sub, chat_id))
        conn.commit()
        await update.message.reply_text(f"✅ Captcha mode set to <b>{sub}</b>.", parse_mode=ParseMode.HTML)
    elif sub == "timeout" and len(args) > 1 and args[1].isdigit():
        t = max(30, min(600, int(args[1])))
        cursor.execute("UPDATE captcha_settings SET timeout=? WHERE chat_id=?", (t, chat_id))
        conn.commit()
        await update.message.reply_text(f"✅ Captcha timeout set to <b>{t}s</b>.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("❓ Unknown sub-command. Use: on | off | button | math | timeout <secs>")


async def captcha_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Called when a new member joins; send captcha challenge if enabled."""
    if not update.message or not update.message.new_chat_members:
        return
    chat_id = update.effective_chat.id
    cursor.execute("SELECT enabled, mode, timeout FROM captcha_settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return
    mode, timeout = row[1], row[2]
    for member in update.message.new_chat_members:
        if member.is_bot:
            continue
        try:
            await context.bot.restrict_chat_member(
                chat_id, member.id,
                permissions=ChatPermissions(can_send_messages=False),
            )
        except Exception:
            pass
        if mode == "math":
            a, b = random.randint(1, 20), random.randint(1, 20)
            answer = a + b
            context.chat_data[f"captcha_ans_{member.id}"] = answer
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(str(random.randint(1, 50)), callback_data=f"cap_{member.id}_{random.randint(1,50)}"),
                InlineKeyboardButton(str(answer), callback_data=f"cap_{member.id}_{answer}"),
                InlineKeyboardButton(str(random.randint(1, 50)), callback_data=f"cap_{member.id}_{random.randint(1,50)}"),
            ]])
            challenge = f"👋 Welcome {mention(member)}!\n\n🔢 <b>Captcha:</b> What is <b>{a} + {b}</b>?\nYou have <b>{timeout}s</b> to answer or you'll be kicked."
        else:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ I'm not a robot!", callback_data=f"captcha_ok_{member.id}")
            ]])
            challenge = f"👋 Welcome {mention(member)}!\n\n🔒 <b>Verification required.</b>\nClick the button below within <b>{timeout}s</b> to join."
        msg = await context.bot.send_message(chat_id, challenge, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        _captcha_pending[(chat_id, member.id)] = msg.message_id
        cursor.execute(
            "INSERT OR REPLACE INTO captcha_pending (chat_id, user_id, message_id, expires_at) VALUES (?,?,?,?)",
            (chat_id, member.id, msg.message_id, int(time.time()) + timeout),
        )
        conn.commit()
        context.application.job_queue.run_once(
            _captcha_expire, when=timeout,
            data=(chat_id, member.id, msg.message_id),
        )


async def _captcha_expire(context: ContextTypes.DEFAULT_TYPE):
    chat_id, user_id, msg_id = context.job.data
    cursor.execute("SELECT expires_at FROM captcha_pending WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cursor.fetchone()
    if not row:
        return
    cursor.execute("DELETE FROM captcha_pending WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    conn.commit()
    try:
        await context.bot.ban_chat_member(chat_id, user_id)
        await context.bot.unban_chat_member(chat_id, user_id)
    except Exception:
        pass
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except Exception:
        pass


async def captcha_button_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    user = query.from_user
    chat_id = query.message.chat.id

    if data.startswith("captcha_ok_"):
        uid = int(data.split("_")[-1])
        if user.id != uid:
            return await query.answer("❌ This isn't your captcha!", show_alert=True)
        await _pass_captcha(context, chat_id, uid, query.message.message_id)
        await query.answer("✅ Verified! Welcome!")

    elif data.startswith("cap_"):
        parts = data.split("_")
        uid = int(parts[1])
        chosen = int(parts[2])
        if user.id != uid:
            return await query.answer("❌ Not your captcha!", show_alert=True)
        correct = context.chat_data.get(f"captcha_ans_{uid}")
        if correct and chosen == correct:
            await _pass_captcha(context, chat_id, uid, query.message.message_id)
            await query.answer("✅ Correct! Welcome!")
        else:
            await query.answer("❌ Wrong answer! Try again or wait to be kicked.", show_alert=True)


async def _pass_captcha(context, chat_id, user_id, msg_id):
    try:
        await context.bot.restrict_chat_member(
            chat_id, user_id,
            permissions=ChatPermissions(
                can_send_messages=True, can_send_photos=True,
                can_send_videos=True, can_send_documents=True,
                can_send_polls=True, can_add_web_page_previews=True,
            ),
        )
    except Exception:
        pass
    cursor.execute("DELETE FROM captcha_pending WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    conn.commit()
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════
#  FORCE SUBSCRIBE
# ══════════════════════════════════════════════════════════
@admin_only
async def forcesub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure force-subscribe: /forcesub <@channel|off>"""
    chat_id = update.effective_chat.id
    if not context.args:
        cursor.execute("SELECT enabled, channel_username FROM force_sub WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        en = row and row[0]
        ch = (row and row[1]) or "Not set"
        return await update.message.reply_text(
            f"📢 <b>Force Subscribe</b>\nStatus: {'✅ On' if en else '❌ Off'}\nChannel: <b>{html.escape(ch)}</b>\n\nUsage: /forcesub @channel | /forcesub off",
            parse_mode=ParseMode.HTML,
        )
    arg = context.args[0].lower()
    if arg == "off":
        cursor.execute("INSERT OR IGNORE INTO force_sub (chat_id) VALUES (?)", (chat_id,))
        cursor.execute("UPDATE force_sub SET enabled=0 WHERE chat_id=?", (chat_id,))
        conn.commit()
        return await update.message.reply_text("❌ Force subscribe <b>disabled</b>.", parse_mode=ParseMode.HTML)
    channel = arg if arg.startswith("@") else f"@{arg}"
    try:
        chat_obj = await context.bot.get_chat(channel)
        channel_id = chat_obj.id
    except Exception:
        return await update.message.reply_text("❌ Couldn't find that channel. Make sure the bot is an admin there.")
    cursor.execute("INSERT OR IGNORE INTO force_sub (chat_id) VALUES (?)", (chat_id,))
    cursor.execute(
        "UPDATE force_sub SET enabled=1, channel_id=?, channel_username=? WHERE chat_id=?",
        (channel_id, channel.lstrip("@"), chat_id),
    )
    conn.commit()
    await update.message.reply_text(f"✅ Force subscribe enabled! Users must join <b>{html.escape(channel)}</b> to chat.", parse_mode=ParseMode.HTML)


async def check_force_sub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Returns True if the message should be deleted (user not subscribed)."""
    if not update.message or not update.effective_user:
        return False
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if await is_admin(update, context, user_id):
        return False
    cursor.execute("SELECT enabled, channel_id, channel_username FROM force_sub WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0] or not row[1]:
        return False
    channel_id, channel_username = row[1], row[2]
    try:
        member = await context.bot.get_chat_member(channel_id, user_id)
        if member.status in ("member", "administrator", "creator", "restricted"):
            return False
    except Exception:
        return False
    try:
        await update.message.delete()
    except Exception:
        pass
    ch = f"@{channel_username}" if channel_username else str(channel_id)
    try:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"📢 Join {ch}", url=f"https://t.me/{channel_username}")]])
        notice = await context.bot.send_message(
            chat_id,
            f"🔔 {mention(update.effective_user)}, you must join {ch} to chat here!",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        await asyncio.sleep(8)
        await notice.delete()
    except Exception:
        pass
    return True


# ══════════════════════════════════════════════════════════
#  ANTI-RAID PROTECTION
# ══════════════════════════════════════════════════════════
_raid_tracker: dict[int, list] = {}   # chat_id -> [join_timestamps]


@admin_only
async def antiraid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure anti-raid: /antiraid on|off|status|set <threshold> <window> <action>"""
    chat_id = update.effective_chat.id
    args = context.args or []
    cursor.execute("INSERT OR IGNORE INTO anti_raid (chat_id) VALUES (?)", (chat_id,))
    if not args:
        cursor.execute("SELECT enabled, threshold, window, action FROM anti_raid WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone() or (0, 5, 10, "kick")
        return await update.message.reply_text(
            f"🛡️ <b>Anti-Raid Settings</b>\n\n"
            f"Status: {'✅ On' if row[0] else '❌ Off'}\n"
            f"Threshold: <b>{row[1]}</b> joins\n"
            f"Window: <b>{row[2]}</b>s\n"
            f"Action: <b>{row[3]}</b>\n\n"
            f"Usage: /antiraid on|off\n"
            f"/antiraid set <threshold> <window_secs> <ban|kick|mute>",
            parse_mode=ParseMode.HTML,
        )
    sub = args[0].lower()
    if sub == "on":
        cursor.execute("UPDATE anti_raid SET enabled=1 WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text("✅ Anti-raid <b>enabled</b>!", parse_mode=ParseMode.HTML)
    elif sub == "off":
        cursor.execute("UPDATE anti_raid SET enabled=0 WHERE chat_id=?", (chat_id,))
        conn.commit()
        await update.message.reply_text("❌ Anti-raid <b>disabled</b>.", parse_mode=ParseMode.HTML)
    elif sub == "set" and len(args) >= 4:
        thr = max(2, int(args[1])) if args[1].isdigit() else 5
        win = max(5, int(args[2])) if args[2].isdigit() else 10
        act = args[3].lower() if args[3].lower() in ("ban", "kick", "mute") else "kick"
        cursor.execute("UPDATE anti_raid SET threshold=?, window=?, action=? WHERE chat_id=?", (thr, win, act, chat_id))
        conn.commit()
        await update.message.reply_text(
            f"✅ Anti-raid: {thr} joins / {win}s → <b>{act}</b>", parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("Usage: /antiraid on|off|set <threshold> <window> <ban|kick|mute>")


async def check_anti_raid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Called on new_chat_members; detects mass join events."""
    if not update.message or not update.message.new_chat_members:
        return
    chat_id = update.effective_chat.id
    cursor.execute("SELECT enabled, threshold, window, action FROM anti_raid WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return
    _, threshold, window, action = row
    now = time.time()
    _raid_tracker.setdefault(chat_id, [])
    _raid_tracker[chat_id] = [t for t in _raid_tracker[chat_id] if now - t < window]
    _raid_tracker[chat_id].extend([now] * len(update.message.new_chat_members))
    if len(_raid_tracker[chat_id]) >= threshold:
        _raid_tracker[chat_id] = []
        for member in update.message.new_chat_members:
            if member.is_bot:
                continue
            try:
                if action == "ban":
                    await context.bot.ban_chat_member(chat_id, member.id)
                elif action == "mute":
                    await context.bot.restrict_chat_member(chat_id, member.id, ChatPermissions(can_send_messages=False))
                else:
                    await context.bot.ban_chat_member(chat_id, member.id)
                    await context.bot.unban_chat_member(chat_id, member.id)
            except Exception:
                pass
        await context.bot.send_message(
            chat_id,
            f"🛡️ <b>Anti-Raid Activated!</b>\nDetected mass join. Action: <b>{action}</b>",
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════
#  KARMA / REP SYSTEM
# ══════════════════════════════════════════════════════════
KARMA_POS_WORDS = ("+1", "thanks", "thank you", "ty", "thx", "nice one", "good job", "well done", "bravo", "👏", "🙏", "❤️", "💯")
KARMA_NEG_WORDS = ("-1", "bad", "shame", "boo", "👎")


async def check_karma(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Auto-detect +1 / -1 karma triggers in replies."""
    if not update.message or not update.message.reply_to_message:
        return
    if update.effective_chat.type == "private":
        return
    text = (update.message.text or "").strip().lower()
    if not text:
        return
    delta = 0
    for w in KARMA_POS_WORDS:
        if w in text:
            delta = 1
            break
    if delta == 0:
        for w in KARMA_NEG_WORDS:
            if w in text:
                delta = -1
                break
    if delta == 0:
        return
    giver = update.effective_user
    receiver = update.message.reply_to_message.from_user
    if not receiver or receiver.id == giver.id or receiver.is_bot:
        return
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO karma (chat_id, user_id) VALUES (?,?)", (chat_id, receiver.id))
    cursor.execute("UPDATE karma SET points=points+? WHERE chat_id=? AND user_id=?", (delta, chat_id, receiver.id))
    conn.commit()
    cursor.execute("SELECT points FROM karma WHERE chat_id=? AND user_id=?", (chat_id, receiver.id))
    row = cursor.fetchone()
    pts = row[0] if row else delta
    emoji = "⬆️" if delta > 0 else "⬇️"
    await update.message.reply_text(
        f"{emoji} {mention(receiver)}'s karma is now <b>{pts:+d}</b>!",
        parse_mode=ParseMode.HTML,
    )


async def karma_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    elif context.args:
        try:
            target = await context.bot.get_chat(int(context.args[0]) if context.args[0].isdigit() else f"@{context.args[0].lstrip('@')}")
        except Exception:
            return await update.message.reply_text("❌ User not found!")
    else:
        target = update.effective_user
    chat_id = update.effective_chat.id
    cursor.execute("SELECT points FROM karma WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    row = cursor.fetchone()
    pts = row[0] if row else 0
    await update.message.reply_text(
        f"⭐ {mention(target)} has <b>{pts:+d} karma</b> in this chat.",
        parse_mode=ParseMode.HTML,
    )


async def ktop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT user_id, points FROM karma WHERE chat_id=? ORDER BY points DESC LIMIT 10", (chat_id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📊 No karma data yet!")
    lines = []
    medals = ["🥇", "🥈", "🥉"]
    for i, (uid, pts) in enumerate(rows):
        medal = medals[i] if i < 3 else f"{i+1}."
        cursor.execute("SELECT first_name FROM user_cache WHERE user_id=?", (uid,))
        r = cursor.fetchone()
        name = html.escape(r[0] if r else str(uid))
        lines.append(f"{medal} <a href='tg://user?id={uid}'>{name}</a> — <b>{pts:+d}</b>")
    await update.message.reply_text(
        f"🏆 <b>Karma Leaderboard</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def kresetall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return await update.message.reply_text(random.choice(NO_PERM_MSGS), parse_mode=ParseMode.HTML)
    cursor.execute("DELETE FROM karma WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("✅ All karma reset for this group!")


# ══════════════════════════════════════════════════════════
#  CUSTOM COMMANDS
# ══════════════════════════════════════════════════════════
@admin_only
async def addcmd_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        return await update.message.reply_text("Usage: /addcmd <command> <response>")
    cmd = context.args[0].lower().lstrip("/")
    response = " ".join(context.args[1:])
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR REPLACE INTO custom_cmds (chat_id, cmd, response) VALUES (?,?,?)", (chat_id, cmd, response))
    conn.commit()
    await update.message.reply_text(f"✅ Command <code>/{html.escape(cmd)}</code> saved!", parse_mode=ParseMode.HTML)


@admin_only
async def rmcmd_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /rmcmd <command>")
    cmd = context.args[0].lower().lstrip("/")
    chat_id = update.effective_chat.id
    cursor.execute("DELETE FROM custom_cmds WHERE chat_id=? AND cmd=?", (chat_id, cmd))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"✅ Command <code>/{html.escape(cmd)}</code> removed!", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ Command <code>/{html.escape(cmd)}</code> not found.", parse_mode=ParseMode.HTML)


async def cmds_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT cmd, response FROM custom_cmds WHERE chat_id=?", (chat_id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("ℹ️ No custom commands set for this group.")
    lines = [f"/<code>{html.escape(cmd)}</code> — {html.escape(resp[:60])}" for cmd, resp in rows]
    await update.message.reply_text(
        f"🤖 <b>Custom Commands ({len(rows)}):</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


async def handle_custom_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check incoming commands against custom_cmds table."""
    if not update.message or not update.message.text:
        return
    text = update.message.text.split()[0].lstrip("/").split("@")[0].lower()
    if not text:
        return
    chat_id = update.effective_chat.id
    cursor.execute("SELECT response FROM custom_cmds WHERE chat_id=? AND cmd=?", (chat_id, text))
    row = cursor.fetchone()
    if row:
        await update.message.reply_text(row[0], parse_mode=ParseMode.HTML)


# ══════════════════════════════════════════════════════════
#  ACTIVITY STATS / LEADERBOARD
# ══════════════════════════════════════════════════════════
async def track_message_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user or update.effective_chat.type == "private":
        return
    if update.effective_user.is_bot:
        return
    chat_id, user_id = update.effective_chat.id, update.effective_user.id
    cursor.execute("INSERT OR IGNORE INTO group_msg_count (chat_id, user_id) VALUES (?,?)", (chat_id, user_id))
    cursor.execute("UPDATE group_msg_count SET count=count+1 WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    conn.commit()


async def topactive_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute(
        "SELECT user_id, count FROM group_msg_count WHERE chat_id=? ORDER BY count DESC LIMIT 15",
        (chat_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📊 No activity tracked yet.")
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, (uid, cnt) in enumerate(rows):
        cursor.execute("SELECT first_name FROM user_cache WHERE user_id=?", (uid,))
        r = cursor.fetchone()
        name = html.escape(r[0] if r else str(uid))
        badge = medals[i] if i < 3 else f"{i+1}."
        lines.append(f"{badge} <a href='tg://user?id={uid}'>{name}</a> — <b>{cnt}</b> msgs")
    await update.message.reply_text(
        f"📊 <b>Top Active Members</b> — {html.escape(update.effective_chat.title or '')}\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def msgcount_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    else:
        target = update.effective_user
    chat_id = update.effective_chat.id
    cursor.execute("SELECT count FROM group_msg_count WHERE chat_id=? AND user_id=?", (chat_id, target.id))
    row = cursor.fetchone()
    cnt = row[0] if row else 0
    await update.message.reply_text(
        f"💬 {mention(target)} has sent <b>{cnt}</b> message(s) in this group.",
        parse_mode=ParseMode.HTML,
    )


async def resetactivity_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return
    cursor.execute("DELETE FROM group_msg_count WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("✅ Activity data reset for this group.")


# ══════════════════════════════════════════════════════════
#  SLOWMODE
# ══════════════════════════════════════════════════════════
@admin_only
async def slowmode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set slowmode via Telegram native API: /slowmode <seconds|off>"""
    chat_id = update.effective_chat.id
    if not context.args:
        return await update.message.reply_text("Usage: /slowmode <seconds> | /slowmode off")
    arg = context.args[0].lower()
    secs = 0
    if arg in ("off", "0"):
        secs = 0
    elif arg.isdigit():
        secs = max(0, min(3600, int(arg)))
    else:
        parsed = parse_time(arg)
        if parsed:
            secs = max(0, min(3600, parsed))
        else:
            return await update.message.reply_text("❌ Invalid time! Use seconds (10) or a time string (10s, 1m).")
    try:
        await context.bot.set_chat_slow_mode_delay(chat_id, secs)
        if secs:
            await update.message.reply_text(f"🐢 Slowmode set to <b>{fmt_secs(secs)}</b>.", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text("✅ Slowmode <b>disabled</b>.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


# ══════════════════════════════════════════════════════════
#  CAPS FILTER
# ══════════════════════════════════════════════════════════
@admin_only
async def capsfilter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure caps filter: /capsfilter on|off|set <min_len> <percent>"""
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO caps_filter (chat_id) VALUES (?)", (chat_id,))
    args = context.args or []
    if not args:
        cursor.execute("SELECT enabled, min_length, percent FROM caps_filter WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone() or (0, 10, 70)
        return await update.message.reply_text(
            f"🔠 <b>Caps Filter</b>\nStatus: {'✅ On' if row[0] else '❌ Off'}\n"
            f"Min length: <b>{row[1]}</b> chars\nThreshold: <b>{row[2]}%</b> uppercase\n\n"
            f"Usage: /capsfilter on|off|set <min_chars> <percent>",
            parse_mode=ParseMode.HTML,
        )
    sub = args[0].lower()
    if sub == "on":
        cursor.execute("UPDATE caps_filter SET enabled=1 WHERE chat_id=?", (chat_id,))
        await update.message.reply_text("✅ Caps filter <b>enabled</b>.", parse_mode=ParseMode.HTML)
    elif sub == "off":
        cursor.execute("UPDATE caps_filter SET enabled=0 WHERE chat_id=?", (chat_id,))
        await update.message.reply_text("❌ Caps filter <b>disabled</b>.", parse_mode=ParseMode.HTML)
    elif sub == "set" and len(args) >= 3:
        ml = max(5, int(args[1])) if args[1].isdigit() else 10
        pct = max(50, min(100, int(args[2]))) if args[2].isdigit() else 70
        cursor.execute("UPDATE caps_filter SET min_length=?, percent=? WHERE chat_id=?", (ml, pct, chat_id))
        await update.message.reply_text(f"✅ Caps filter: messages ≥{ml} chars, ≥{pct}% uppercase will be deleted.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Usage: /capsfilter on|off|set <min_len> <percent>")
    conn.commit()


async def check_caps_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if update.effective_chat.type == "private":
        return
    user_id = update.effective_user.id
    if await is_admin(update, context, user_id):
        return
    if await is_approved(update.effective_chat.id, user_id):
        return
    chat_id = update.effective_chat.id
    cursor.execute("SELECT enabled, min_length, percent FROM caps_filter WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return
    _, min_len, threshold = row
    text = update.message.text
    if len(text) < min_len:
        return
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return
    upper_pct = sum(1 for c in letters if c.isupper()) * 100 // len(letters)
    if upper_pct >= threshold:
        try:
            await update.message.delete()
            m = await context.bot.send_message(
                chat_id,
                f"🔠 {mention(update.effective_user)}, please don't use excessive caps!",
                parse_mode=ParseMode.HTML,
            )
            await asyncio.sleep(5)
            await m.delete()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  EMOJI FILTER
# ══════════════════════════════════════════════════════════
@admin_only
async def emojifilter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configure emoji filter: /emojifilter on|off|set <max_count>"""
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO emoji_filter (chat_id) VALUES (?)", (chat_id,))
    args = context.args or []
    if not args:
        cursor.execute("SELECT enabled, max_count FROM emoji_filter WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone() or (0, 5)
        return await update.message.reply_text(
            f"😀 <b>Emoji Filter</b>\nStatus: {'✅ On' if row[0] else '❌ Off'}\nMax emojis: <b>{row[1]}</b>\n\n"
            f"Usage: /emojifilter on|off|set <max>",
            parse_mode=ParseMode.HTML,
        )
    sub = args[0].lower()
    if sub == "on":
        cursor.execute("UPDATE emoji_filter SET enabled=1 WHERE chat_id=?", (chat_id,))
        await update.message.reply_text("✅ Emoji filter enabled.", parse_mode=ParseMode.HTML)
    elif sub == "off":
        cursor.execute("UPDATE emoji_filter SET enabled=0 WHERE chat_id=?", (chat_id,))
        await update.message.reply_text("❌ Emoji filter disabled.", parse_mode=ParseMode.HTML)
    elif sub == "set" and len(args) >= 2 and args[1].isdigit():
        mc = max(1, int(args[1]))
        cursor.execute("UPDATE emoji_filter SET max_count=? WHERE chat_id=?", (mc, chat_id))
        await update.message.reply_text(f"✅ Max emojis per message: <b>{mc}</b>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Usage: /emojifilter on|off|set <max_count>")
    conn.commit()


def _count_emojis(text: str) -> int:
    import unicodedata
    count = 0
    for ch in text:
        if unicodedata.category(ch) in ("So", "Sm") or ord(ch) > 127000:
            count += 1
    return count


async def check_emoji_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if update.effective_chat.type == "private":
        return
    user_id = update.effective_user.id
    if await is_admin(update, context, user_id):
        return
    if await is_approved(update.effective_chat.id, user_id):
        return
    chat_id = update.effective_chat.id
    cursor.execute("SELECT enabled, max_count FROM emoji_filter WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return
    text = update.message.text or update.message.caption or ""
    if _count_emojis(text) > row[1]:
        try:
            await update.message.delete()
            m = await context.bot.send_message(
                chat_id,
                f"😀 {mention(update.effective_user)}, too many emojis! Max allowed: <b>{row[1]}</b>",
                parse_mode=ParseMode.HTML,
            )
            await asyncio.sleep(5)
            await m.delete()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  LINK WHITELIST
# ══════════════════════════════════════════════════════════
@admin_only
async def allowlink_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /allowlink <domain.com>")
    domain = context.args[0].lower().lstrip("https://").lstrip("http://").split("/")[0]
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO link_whitelist (chat_id, domain) VALUES (?,?)", (chat_id, domain))
    conn.commit()
    await update.message.reply_text(f"✅ <code>{html.escape(domain)}</code> allowed.", parse_mode=ParseMode.HTML)


@admin_only
async def rmlink_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /rmlink <domain.com>")
    domain = context.args[0].lower()
    chat_id = update.effective_chat.id
    cursor.execute("DELETE FROM link_whitelist WHERE chat_id=? AND domain=?", (chat_id, domain))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"✅ <code>{html.escape(domain)}</code> removed.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ Domain not in whitelist.")


async def allowedlinks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT domain FROM link_whitelist WHERE chat_id=?", (chat_id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("ℹ️ No link whitelist configured. All links obey the locks setting.")
    lst = "\n".join(f"• <code>{html.escape(r[0])}</code>" for r in rows)
    await update.message.reply_text(f"🔗 <b>Whitelisted Domains:</b>\n\n{lst}", parse_mode=ParseMode.HTML)


# ══════════════════════════════════════════════════════════
#  ANTI-FORWARD
# ══════════════════════════════════════════════════════════
@admin_only
async def antiforward_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO anti_forward (chat_id) VALUES (?)", (chat_id,))
    args = context.args or []
    if not args:
        cursor.execute("SELECT enabled FROM anti_forward WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        st = "✅ On" if (row and row[0]) else "❌ Off"
        return await update.message.reply_text(
            f"↩️ <b>Anti-Forward</b>: {st}\n\nUsage: /antiforward on|off",
            parse_mode=ParseMode.HTML,
        )
    sub = args[0].lower()
    val = 1 if sub == "on" else 0
    cursor.execute("UPDATE anti_forward SET enabled=? WHERE chat_id=?", (val, chat_id))
    conn.commit()
    await update.message.reply_text(
        f"↩️ Anti-forward {'<b>enabled</b>' if val else '<b>disabled</b>'}.",
        parse_mode=ParseMode.HTML,
    )


async def check_anti_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.effective_chat.type == "private":
        return
    if not update.message.forward_origin and not update.message.forward_date:
        return
    user_id = update.effective_user.id
    if await is_admin(update, context, user_id):
        return
    if await is_approved(update.effective_chat.id, user_id):
        return
    chat_id = update.effective_chat.id
    cursor.execute("SELECT enabled FROM anti_forward WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if row and row[0]:
        try:
            await update.message.delete()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  QUOTE FEATURE
# ══════════════════════════════════════════════════════════
async def quote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save or retrieve a random quote: /quote [#id]"""
    chat_id = update.effective_chat.id
    # Retrieve by ID
    if context.args and context.args[0].startswith("#"):
        qid_str = context.args[0][1:]
        if qid_str.isdigit():
            cursor.execute("SELECT user_name, content, added_at FROM quotes WHERE chat_id=? AND quote_id=?", (chat_id, int(qid_str)))
            row = cursor.fetchone()
            if row:
                return await update.message.reply_text(
                    f"💬 <i>{html.escape(row[1])}</i>\n— {html.escape(row[0])} | #{qid_str}",
                    parse_mode=ParseMode.HTML,
                )
            return await update.message.reply_text(f"❌ Quote #{qid_str} not found.")

    # Save quoted message
    if update.message.reply_to_message:
        src = update.message.reply_to_message
        author = src.from_user
        content = src.text or src.caption or ""
        if not content:
            return await update.message.reply_text("❌ Can only quote text messages.")
        cursor.execute("SELECT COALESCE(MAX(quote_id), 0) + 1 FROM quotes WHERE chat_id=?", (chat_id,))
        new_id = cursor.fetchone()[0]
        uname = f"@{author.username}" if author.username else html.escape(author.first_name or "Unknown")
        cursor.execute(
            "INSERT INTO quotes (chat_id, quote_id, user_id, user_name, content, added_at) VALUES (?,?,?,?,?,?)",
            (chat_id, new_id, author.id, uname, content, int(time.time())),
        )
        conn.commit()
        return await update.message.reply_text(
            f"✅ Quote saved as <b>#{new_id}</b>!\n\n💬 <i>{html.escape(content[:200])}</i>\n— {html.escape(uname)}",
            parse_mode=ParseMode.HTML,
        )

    # Random quote
    cursor.execute("SELECT quote_id, user_name, content FROM quotes WHERE chat_id=? ORDER BY RANDOM() LIMIT 1", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("📭 No quotes saved yet! Reply to a message with /quote to save one.")
    await update.message.reply_text(
        f"💬 <i>{html.escape(row[2])}</i>\n— {html.escape(row[1])} | #{row[0]}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def delquote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].lstrip("#").isdigit():
        return await update.message.reply_text("Usage: /delquote #<id>")
    qid = int(context.args[0].lstrip("#"))
    chat_id = update.effective_chat.id
    cursor.execute("DELETE FROM quotes WHERE chat_id=? AND quote_id=?", (chat_id, qid))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"✅ Quote #{qid} deleted.")
    else:
        await update.message.reply_text(f"❌ Quote #{qid} not found.")


async def quotes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT COUNT(*) FROM quotes WHERE chat_id=?", (chat_id,))
    n = cursor.fetchone()[0]
    await update.message.reply_text(
        f"💬 This group has <b>{n}</b> saved quote(s).\nUse /quote to get a random one, /quote #<id> to get by ID.",
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  REMINDERS
# ══════════════════════════════════════════════════════════
async def remind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set a reminder: /remind <time> <text>"""
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text("Usage: /remind <time> <text>\nExample: /remind 30m Take a break!")
    secs = parse_time(args[0])
    if not secs:
        return await update.message.reply_text("❌ Invalid time. Use: 30s, 5m, 2h, 1d")
    text = " ".join(args[1:])
    fire_at = int(time.time()) + secs
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    cursor.execute(
        "INSERT INTO reminders (chat_id, user_id, fire_at, text) VALUES (?,?,?,?)",
        (chat_id, user_id, fire_at, text),
    )
    conn.commit()
    rid = cursor.lastrowid
    context.application.job_queue.run_once(
        _fire_reminder, when=secs, data=(rid, chat_id, user_id, text)
    )
    await update.message.reply_text(
        f"⏰ Reminder #{rid} set! I'll remind you in <b>{fmt_secs(secs)}</b>: <i>{html.escape(text[:200])}</i>",
        parse_mode=ParseMode.HTML,
    )


async def _fire_reminder(context: ContextTypes.DEFAULT_TYPE):
    rid, chat_id, user_id, text = context.job.data
    cursor.execute("UPDATE reminders SET done=1 WHERE reminder_id=?", (rid,))
    conn.commit()
    try:
        await context.bot.send_message(
            chat_id,
            f"⏰ <b>Reminder #{rid}</b> for <a href='tg://user?id={user_id}'>you</a>:\n\n<i>{html.escape(text)}</i>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


async def reminders_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    now = int(time.time())
    cursor.execute(
        "SELECT reminder_id, fire_at, text FROM reminders WHERE chat_id=? AND user_id=? AND done=0 AND fire_at>?",
        (chat_id, user_id, now),
    )
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📭 You have no active reminders in this chat.")
    lines = []
    for rid, fire_at, text in rows:
        remaining = fire_at - now
        lines.append(f"• #{rid} — in <b>{fmt_secs(remaining)}</b>: <i>{html.escape(text[:60])}</i>")
    await update.message.reply_text(
        f"⏰ <b>Your Reminders:</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


async def cancelreminder_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Usage: /cancelreminder <id>")
    rid = int(context.args[0])
    user_id = update.effective_user.id
    cursor.execute("UPDATE reminders SET done=1 WHERE reminder_id=? AND user_id=?", (rid, user_id))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"✅ Reminder #{rid} cancelled.")
    else:
        await update.message.reply_text(f"❌ Reminder #{rid} not found or doesn't belong to you.")


# ══════════════════════════════════════════════════════════
#  SCHEDULED MESSAGES
# ══════════════════════════════════════════════════════════
@admin_only
async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Schedule a message: /schedule <time> <text>"""
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_text(
            "Usage: /schedule <time> <text>\nExample: /schedule 1h Good morning everyone!"
        )
    secs = parse_time(args[0])
    if not secs:
        return await update.message.reply_text("❌ Invalid time!")
    text = " ".join(args[1:])
    fire_at = int(time.time()) + secs
    chat_id = update.effective_chat.id
    cursor.execute("INSERT INTO scheduled_msgs (chat_id, text, fire_at) VALUES (?,?,?)", (chat_id, text, fire_at))
    conn.commit()
    sid = cursor.lastrowid
    context.application.job_queue.run_once(
        _fire_scheduled_msg, when=secs, data=(sid, chat_id, text)
    )
    await update.message.reply_text(
        f"📅 Message #{sid} scheduled in <b>{fmt_secs(secs)}</b>: <i>{html.escape(text[:150])}</i>",
        parse_mode=ParseMode.HTML,
    )


async def _fire_scheduled_msg(context: ContextTypes.DEFAULT_TYPE):
    sid, chat_id, text = context.job.data
    cursor.execute("UPDATE scheduled_msgs SET done=1 WHERE id=?", (sid,))
    conn.commit()
    try:
        await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    except Exception:
        pass


@admin_only
async def cancelschedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Usage: /cancelschedule <id>")
    sid = int(context.args[0])
    chat_id = update.effective_chat.id
    cursor.execute("UPDATE scheduled_msgs SET done=1 WHERE id=? AND chat_id=?", (sid, chat_id))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"✅ Scheduled message #{sid} cancelled.")
    else:
        await update.message.reply_text(f"❌ Message #{sid} not found.")


# ══════════════════════════════════════════════════════════
#  AUTO-DELETE
# ══════════════════════════════════════════════════════════
@admin_only
async def autodelete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Auto-delete all messages after N seconds: /autodelete <secs|off>"""
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO auto_delete (chat_id) VALUES (?)", (chat_id,))
    if not context.args:
        cursor.execute("SELECT seconds FROM auto_delete WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        secs = row[0] if row else 0
        return await update.message.reply_text(
            f"🗑️ Auto-delete: {'<b>' + fmt_secs(secs) + '</b>' if secs else '<b>off</b>'}\n\n"
            f"Usage: /autodelete <seconds> | /autodelete off",
            parse_mode=ParseMode.HTML,
        )
    arg = context.args[0].lower()
    if arg == "off":
        secs = 0
    else:
        secs = parse_time(arg) if not arg.isdigit() else int(arg)
        if not secs:
            return await update.message.reply_text("❌ Invalid time.")
        secs = max(5, min(86400, secs))
    cursor.execute("UPDATE auto_delete SET seconds=? WHERE chat_id=?", (secs, chat_id))
    conn.commit()
    if secs:
        await update.message.reply_text(f"🗑️ Auto-delete set to <b>{fmt_secs(secs)}</b>.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("✅ Auto-delete <b>disabled</b>.", parse_mode=ParseMode.HTML)


async def check_auto_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.effective_chat.type == "private":
        return
    chat_id = update.effective_chat.id
    cursor.execute("SELECT seconds FROM auto_delete WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return
    secs = row[0]
    msg = update.message

    async def _delete_later():
        await asyncio.sleep(secs)
        try:
            await msg.delete()
        except Exception:
            pass

    asyncio.create_task(_delete_later())


# ══════════════════════════════════════════════════════════
#  CHAT BACKUP / RESTORE
# ══════════════════════════════════════════════════════════
@admin_only
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Backup group settings to JSON."""
    chat_id = update.effective_chat.id
    data: dict = {}

    # Settings
    cursor.execute("SELECT * FROM settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if row:
        cols = [d[0] for d in cursor.description]
        data["settings"] = dict(zip(cols, row))

    # Welcome
    cursor.execute("SELECT * FROM welcome WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if row:
        cols = [d[0] for d in cursor.description]
        data["welcome"] = dict(zip(cols, row))

    # Rules
    cursor.execute("SELECT * FROM rules WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if row:
        cols = [d[0] for d in cursor.description]
        data["rules"] = dict(zip(cols, row))

    # Locks
    cursor.execute("SELECT * FROM locks WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if row:
        cols = [d[0] for d in cursor.description]
        data["locks"] = dict(zip(cols, row))

    # Flood
    cursor.execute("SELECT * FROM flood WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if row:
        cols = [d[0] for d in cursor.description]
        data["flood"] = dict(zip(cols, row))

    # Notes
    cursor.execute("SELECT name, content FROM notes WHERE chat_id=?", (chat_id,))
    data["notes"] = [{"name": r[0], "content": r[1]} for r in cursor.fetchall()]

    # Blacklist
    cursor.execute("SELECT word FROM blacklist WHERE chat_id=?", (chat_id,))
    data["blacklist"] = [r[0] for r in cursor.fetchall()]

    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    cursor.execute(
        "INSERT OR REPLACE INTO chat_backup (chat_id, data, backed_up_at) VALUES (?,?,?)",
        (chat_id, json_str, int(time.time())),
    )
    conn.commit()

    import io
    buf = io.BytesIO(json_str.encode())
    buf.name = f"backup_{abs(chat_id)}.json"
    await update.message.reply_document(buf, caption="📦 Group backup created!")


@admin_only
async def restore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore group settings from backup: reply to the backup JSON document."""
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        # Try last backup from DB
        cursor.execute("SELECT data FROM chat_backup WHERE chat_id=?", (update.effective_chat.id,))
        row = cursor.fetchone()
        if not row:
            return await update.message.reply_text("❌ No backup found. Reply to a backup JSON file with /restore.")
        json_str = row[0]
    else:
        doc = update.message.reply_to_message.document
        if doc.file_size and doc.file_size > 200_000:
            return await update.message.reply_text("❌ File too large!")
        tg_file = await doc.get_file()
        import io
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        json_str = buf.getvalue().decode()
    try:
        data = json.loads(json_str)
    except Exception:
        return await update.message.reply_text("❌ Invalid JSON backup file.")

    chat_id = update.effective_chat.id
    restored = []

    if "settings" in data:
        d = data["settings"]
        d.pop("chat_id", None)
        cols = ", ".join(d.keys())
        vals = ", ".join(["?"] * len(d))
        cursor.execute(f"INSERT OR REPLACE INTO settings (chat_id, {cols}) VALUES (?, {vals})", (chat_id, *d.values()))
        restored.append("settings")

    if "welcome" in data:
        d = data["welcome"]
        d.pop("chat_id", None)
        cols = ", ".join(d.keys())
        vals = ", ".join(["?"] * len(d))
        cursor.execute(f"INSERT OR REPLACE INTO welcome (chat_id, {cols}) VALUES (?, {vals})", (chat_id, *d.values()))
        restored.append("welcome")

    if "rules" in data:
        d = data["rules"]
        d.pop("chat_id", None)
        cursor.execute("INSERT OR REPLACE INTO rules (chat_id, rules_text) VALUES (?,?)", (chat_id, d.get("rules_text", "")))
        restored.append("rules")

    if "locks" in data:
        d = data["locks"]
        d.pop("chat_id", None)
        cols = ", ".join(d.keys())
        vals = ", ".join(["?"] * len(d))
        cursor.execute(f"INSERT OR REPLACE INTO locks (chat_id, {cols}) VALUES (?, {vals})", (chat_id, *d.values()))
        restored.append("locks")

    if "notes" in data:
        for note in data["notes"]:
            cursor.execute(
                "INSERT OR REPLACE INTO notes (chat_id, name, content) VALUES (?,?,?)",
                (chat_id, note["name"], note["content"]),
            )
        restored.append(f"{len(data['notes'])} notes")

    if "blacklist" in data:
        for word in data["blacklist"]:
            cursor.execute("INSERT OR IGNORE INTO blacklist (chat_id, word) VALUES (?,?)", (chat_id, word))
        restored.append(f"{len(data['blacklist'])} blacklist entries")

    conn.commit()
    await update.message.reply_text(
        f"✅ Backup restored: {', '.join(restored)}", parse_mode=ParseMode.HTML
    )


# ══════════════════════════════════════════════════════════
#  FUN COMMANDS
# ══════════════════════════════════════════════════════════
EIGHTBALL_REPLIES = [
    "🎱 It is certain!", "🎱 Without a doubt!", "🎱 Yes, definitely!", "🎱 You may rely on it.",
    "🎱 Most likely.", "🎱 Outlook good.", "🎱 Signs point to yes.", "🎱 Reply hazy, try again.",
    "🎱 Ask again later.", "🎱 Better not tell you now.", "🎱 Cannot predict now.",
    "🎱 Concentrate and ask again.", "🎱 Don't count on it.", "🎱 My reply is no.",
    "🎱 My sources say no.", "🎱 Outlook not so good.", "🎱 Very doubtful.", "🎱 Absolutely not!",
    "🎱 The stars say NO.", "🎱 Even the void says no.", "🎱 100% yes, go for it!",
    "🎱 Nope, not in this lifetime.", "🎱 My gut says yes, but my brain says check again.",
    "🎱 The universe says: maybe if you try harder.",
]
ROAST_REPLIES = [
    "I'd roast {user}, but my mom said I'm not allowed to burn trash.",
    "{user}'s brain cells are playing hide and seek — none of them found anything yet.",
    "If brains were gasoline, {user} wouldn't have enough to power a firefly.",
    "{user} has the intellectual depth of a puddle in the Sahara.",
    "Looking at {user}, evolution is clearly still a work in progress.",
    "{user} is living proof that even plants can grow without a brain.",
    "They say every person has a unique gift — {user}'s is still lost in shipping.",
    "{user}'s Wi-Fi password is stronger than their argument.",
    "I've seen smarter things come out of a suggestion box.",
    "{user}, if common sense were rain, you'd die of drought.",
    "Scientists confirmed: {user} is the missing link between humanity and a speed bump.",
    "{user} has the charisma of a soggy cardboard box.",
]
COMPLIMENT_REPLIES = [
    "{user} is the reason I wake up smiling every day! 😊",
    "If kindness had a face, it would look like {user}. ❤️",
    "{user}, you radiate energy that lights up every room you walk into! ✨",
    "{user} is the kind of person who makes the world a better place just by existing.",
    "Brain, beauty, and brilliance — {user} has it all! 💫",
    "{user}, your positivity is absolutely contagious! Keep shining! 🌟",
    "Nobody does it quite like {user}. Truly one of a kind! 💎",
    "{user} has a heart of gold and a mind to match! 🏆",
]
RPS_MAP = {"rock": "🪨", "paper": "📄", "scissors": "✂️"}
RPS_BEATS = {"rock": "scissors", "paper": "rock", "scissors": "paper"}

TRIVIA_QUESTIONS = [
    ("What is the capital of France?", "paris"),
    ("How many sides does a hexagon have?", "6"),
    ("What planet is known as the Red Planet?", "mars"),
    ("Who wrote Romeo and Juliet?", "shakespeare"),
    ("What is 12 × 12?", "144"),
    ("What is the largest ocean on Earth?", "pacific"),
    ("How many minutes are in a day?", "1440"),
    ("What country has the most pyramids?", "sudan"),
    ("What language has the most native speakers?", "mandarin"),
    ("How many bones are in the adult human body?", "206"),
    ("What element does 'O' stand for on the periodic table?", "oxygen"),
    ("What is the tallest mountain in the world?", "everest"),
    ("What is the fastest land animal?", "cheetah"),
    ("In what year did World War II end?", "1945"),
    ("What is the chemical symbol for gold?", "au"),
    ("How many strings does a standard guitar have?", "6"),
    ("What gas do plants absorb from the atmosphere?", "carbon dioxide"),
    ("What is the square root of 169?", "13"),
    ("What is the longest river in the world?", "nile"),
    ("How many continents are there on Earth?", "7"),
]
_trivia_active: dict[int, dict] = {}


async def coinflip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = random.choice(["🪙 Heads!", "🪙 Tails!"])
    await update.message.reply_text(result)


async def dice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sides = 6
    if context.args and context.args[0].isdigit():
        sides = max(2, min(100, int(context.args[0])))
    result = random.randint(1, sides)
    await update.message.reply_text(f"🎲 You rolled a <b>{result}</b> (d{sides})!", parse_mode=ParseMode.HTML)


async def eightball_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    question = " ".join(context.args) if context.args else (
        update.message.reply_to_message.text if update.message.reply_to_message else ""
    )
    if not question.strip():
        return await update.message.reply_text("❓ Ask me a question first! E.g. /8ball Will I get rich?")
    answer = random.choice(EIGHTBALL_REPLIES)
    await update.message.reply_text(
        f"❓ <i>{html.escape(question)}</i>\n\n{answer}", parse_mode=ParseMode.HTML
    )


async def rps_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0].lower() not in RPS_MAP:
        return await update.message.reply_text("Usage: /rps rock|paper|scissors")
    choice = context.args[0].lower()
    bot_choice = random.choice(list(RPS_MAP.keys()))
    user_emoji = RPS_MAP[choice]
    bot_emoji  = RPS_MAP[bot_choice]
    if choice == bot_choice:
        result = "🤝 It's a <b>draw</b>!"
    elif RPS_BEATS[choice] == bot_choice:
        result = "🎉 You <b>win</b>!"
    else:
        result = "😈 I <b>win</b>!"
    await update.message.reply_text(
        f"You: {user_emoji} <b>{choice.capitalize()}</b>\nMe: {bot_emoji} <b>{bot_choice.capitalize()}</b>\n\n{result}",
        parse_mode=ParseMode.HTML,
    )


async def roast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    elif context.args:
        target = update.effective_user
    else:
        target = update.effective_user
    name = html.escape(target.first_name or "you")
    roast = random.choice(ROAST_REPLIES).format(user=f"<b>{name}</b>")
    await update.message.reply_text(f"🔥 {roast}", parse_mode=ParseMode.HTML)


async def compliment_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    else:
        target = update.effective_user
    name = html.escape(target.first_name or "you")
    comp = random.choice(COMPLIMENT_REPLIES).format(user=f"<b>{name}</b>")
    await update.message.reply_text(f"💐 {comp}", parse_mode=ParseMode.HTML)


async def hug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    hugs = ["(っ◔◡◔)っ ❤️", "＼(ˆ▿ˆ)/", "（ っ ^▿^）っ 💕", "(づ｡◕‿‿◕｡)づ", "(⊃｡•́‿•̀｡)⊃"]
    if update.message.reply_to_message:
        target = mention(update.message.reply_to_message.from_user)
    else:
        target = "everyone"
    await update.message.reply_text(f"{mention(update.effective_user)} hugs {target}! {random.choice(hugs)}", parse_mode=ParseMode.HTML)


async def slap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    slaps = [
        "{giver} slaps {target} with a wet noodle! 🍜",
        "{giver} gives {target} a legendary slap! 💥",
        "{giver} smacks {target} with a rubber chicken! 🐔",
        "{giver} whacks {target} with a newspaper! 📰",
        "{giver} karate-chops {target}! ✋",
    ]
    giver = mention(update.effective_user)
    if update.message.reply_to_message:
        target = mention(update.message.reply_to_message.from_user)
    else:
        target = "the air"
    await update.message.reply_text(
        random.choice(slaps).format(giver=giver, target=target), parse_mode=ParseMode.HTML
    )


async def pat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pats = ["(ﾉ◕ヮ◕)ﾉ*:･ﾟ✧", "( ˘ ³˘)♥", "*pats*", "^・ω・^"]
    if update.message.reply_to_message:
        target = mention(update.message.reply_to_message.from_user)
    else:
        target = "everyone"
    await update.message.reply_text(f"{mention(update.effective_user)} pats {target}! {random.choice(pats)}", parse_mode=ParseMode.HTML)


async def ship_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        p1 = html.escape(update.effective_user.first_name or "??")
        p2 = html.escape(update.message.reply_to_message.from_user.first_name or "??")
    elif context.args:
        p1 = html.escape(update.effective_user.first_name or "??")
        p2 = html.escape(" ".join(context.args))
    else:
        return await update.message.reply_text("Usage: /ship @user or reply to someone with /ship")
    compat = random.randint(1, 100)
    bar_len = compat // 10
    bar = "💗" * bar_len + "🖤" * (10 - bar_len)
    await update.message.reply_text(
        f"💘 <b>{p1}</b> + <b>{p2}</b>\n\n{bar}\nCompatibility: <b>{compat}%</b>!",
        parse_mode=ParseMode.HTML,
    )


async def trivia_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in _trivia_active:
        return await update.message.reply_text("⏳ A trivia question is already active! Answer it first.")
    q, ans = random.choice(TRIVIA_QUESTIONS)
    _trivia_active[chat_id] = {"answer": ans, "question": q}
    await update.message.reply_text(
        f"🎓 <b>Trivia Time!</b>\n\n❓ {html.escape(q)}\n\n<i>Type your answer!</i>",
        parse_mode=ParseMode.HTML,
    )
    context.application.job_queue.run_once(
        _trivia_timeout, when=30, data=(chat_id, ans)
    )


async def _trivia_timeout(context: ContextTypes.DEFAULT_TYPE):
    chat_id, ans = context.job.data
    if chat_id in _trivia_active:
        _trivia_active.pop(chat_id, None)
        await context.bot.send_message(
            chat_id,
            f"⏰ Time's up! The answer was: <b>{html.escape(str(ans))}</b>",
            parse_mode=ParseMode.HTML,
        )


async def check_trivia(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or update.effective_chat.type == "private":
        return
    chat_id = update.effective_chat.id
    if chat_id not in _trivia_active:
        return
    guess = update.message.text.strip().lower()
    ans   = str(_trivia_active[chat_id]["answer"]).lower()
    if guess == ans or ans in guess:
        _trivia_active.pop(chat_id, None)
        user  = update.effective_user
        cursor.execute("INSERT OR IGNORE INTO trivia_scores (chat_id, user_id) VALUES (?,?)", (chat_id, user.id))
        cursor.execute("UPDATE trivia_scores SET score=score+1 WHERE chat_id=? AND user_id=?", (chat_id, user.id))
        conn.commit()
        cursor.execute("SELECT score FROM trivia_scores WHERE chat_id=? AND user_id=?", (chat_id, user.id))
        score = cursor.fetchone()[0]
        await update.message.reply_text(
            f"🎉 {mention(user)} got it right! Answer: <b>{html.escape(str(ans))}</b>\n"
            f"🏆 Total score: <b>{score}</b>",
            parse_mode=ParseMode.HTML,
        )


async def triviascore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute(
        "SELECT user_id, score FROM trivia_scores WHERE chat_id=? ORDER BY score DESC LIMIT 10",
        (chat_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📊 No trivia scores yet! Use /trivia to start.")
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, (uid, score) in enumerate(rows):
        cursor.execute("SELECT first_name FROM user_cache WHERE user_id=?", (uid,))
        r = cursor.fetchone()
        name = html.escape(r[0] if r else str(uid))
        badge = medals[i] if i < 3 else f"{i+1}."
        lines.append(f"{badge} <a href='tg://user?id={uid}'>{name}</a> — <b>{score} pts</b>")
    await update.message.reply_text(
        f"🎓 <b>Trivia Leaderboard</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def roll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Roll dice: /roll [NdM] e.g. /roll 2d6"""
    expr = (context.args[0] if context.args else "1d6").lower()
    m = re.match(r"(\d+)d(\d+)", expr)
    if m:
        n, sides = min(int(m.group(1)), 20), min(int(m.group(2)), 100)
    else:
        n, sides = 1, 6
    rolls = [random.randint(1, sides) for _ in range(n)]
    total = sum(rolls)
    roll_str = " + ".join(str(r) for r in rolls)
    await update.message.reply_text(
        f"🎲 <b>{n}d{sides}</b>: [{roll_str}] = <b>{total}</b>",
        parse_mode=ParseMode.HTML,
    )


async def choose_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Randomly pick from options: /choose a | b | c"""
    if not context.args:
        return await update.message.reply_text("Usage: /choose option1 | option2 | option3")
    text = " ".join(context.args)
    opts = [o.strip() for o in text.split("|") if o.strip()]
    if len(opts) < 2:
        opts = text.split()
    if not opts:
        return await update.message.reply_text("❌ Give me at least two options separated by |")
    pick = random.choice(opts)
    await update.message.reply_text(
        f"🎯 I choose: <b>{html.escape(pick)}</b>!", parse_mode=ParseMode.HTML
    )


async def rate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_name = (
        html.escape(update.message.reply_to_message.from_user.first_name)
        if update.message.reply_to_message
        else " ".join(context.args) if context.args
        else html.escape(update.effective_user.first_name or "you")
    )
    score = random.randint(1, 100)
    bar = "⭐" * (score // 10)
    await update.message.reply_text(
        f"⭐ I rate <b>{html.escape(target_name)}</b>: <b>{score}/100</b>\n{bar}",
        parse_mode=ParseMode.HTML,
    )


async def pp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_name = (
        html.escape(update.message.reply_to_message.from_user.first_name)
        if update.message.reply_to_message
        else html.escape(update.effective_user.first_name or "you")
    )
    size = random.randint(1, 30)
    bar  = "█" * size
    await update.message.reply_text(
        f"📏 <b>{target_name}</b>'s pp size:\n{bar} {size} cm",
        parse_mode=ParseMode.HTML,
    )


async def gay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_name = (
        html.escape(update.message.reply_to_message.from_user.first_name)
        if update.message.reply_to_message
        else html.escape(update.effective_user.first_name or "you")
    )
    pct = random.randint(0, 100)
    bar = "🏳️‍🌈" * (pct // 10)
    await update.message.reply_text(
        f"🏳️‍🌈 <b>{target_name}</b> is <b>{pct}% gay</b>!\n{bar}",
        parse_mode=ParseMode.HTML,
    )


async def iq_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_name = (
        html.escape(update.message.reply_to_message.from_user.first_name)
        if update.message.reply_to_message
        else html.escape(update.effective_user.first_name or "you")
    )
    iq = random.randint(40, 200)
    lvl = (
        "🧠 Genius" if iq > 160 else
        "🎓 Above average" if iq > 120 else
        "👍 Average" if iq > 90 else
        "🤔 Below average" if iq > 60 else
        "🥴 Concerningly low"
    )
    await update.message.reply_text(
        f"🧠 <b>{target_name}</b>'s IQ: <b>{iq}</b>\nLevel: {lvl}",
        parse_mode=ParseMode.HTML,
    )


async def howcringe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_name = (
        html.escape(update.message.reply_to_message.from_user.first_name)
        if update.message.reply_to_message
        else html.escape(update.effective_user.first_name or "you")
    )
    pct = random.randint(0, 100)
    await update.message.reply_text(
        f"😬 <b>{target_name}</b> is <b>{pct}% cringe</b>!", parse_mode=ParseMode.HTML
    )


async def love_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return await update.message.reply_text("Reply to someone to calculate love compatibility!")
    p1 = html.escape(update.effective_user.first_name or "??")
    p2 = html.escape(update.message.reply_to_message.from_user.first_name or "??")
    pct = random.randint(1, 100)
    hearts = "❤️" * (pct // 10)
    msg = (
        "💞 Perfect match! Soulmates!" if pct > 90 else
        "💕 Very compatible!" if pct > 70 else
        "💛 Pretty decent!" if pct > 50 else
        "💔 Meh, could work with effort." if pct > 30 else
        "🖤 This is... concerning."
    )
    await update.message.reply_text(
        f"💘 <b>{p1}</b> ❤️ <b>{p2}</b>\n\n{hearts}\nLove meter: <b>{pct}%</b>\n{msg}",
        parse_mode=ParseMode.HTML,
    )


async def truth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    truths = [
        "What's your biggest secret?",
        "Who do you have a crush on in this group?",
        "What's the most embarrassing thing you've ever done?",
        "Have you ever lied to someone close to you? What was it?",
        "What's your most irrational fear?",
        "What's a bad habit you have?",
        "If you could change one thing about yourself, what would it be?",
        "What's the worst date you've ever been on?",
        "Have you ever cheated on a test?",
        "What's your most unpopular opinion?",
        "What's the pettiest thing you've done?",
        "Who in this group do you dislike the most?",
    ]
    await update.message.reply_text(
        f"🎭 <b>Truth:</b> {random.choice(truths)}",
        parse_mode=ParseMode.HTML,
    )


async def dare_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dares = [
        "Send a voice message saying 'I love cats more than people'.",
        "Change your profile picture to something embarrassing for 1 hour.",
        "Write a love poem for the person above you.",
        "Type the next 5 messages with your eyes closed.",
        "Send a funny meme right now.",
        "Confess your most embarrassing crush.",
        "Do your best impression of the group admin.",
        "Say something nice about everyone who has messaged today.",
        "Tell everyone here one thing you appreciate about them.",
        "Send a screenshot of your most recent Google search.",
    ]
    await update.message.reply_text(
        f"🎭 <b>Dare:</b> {random.choice(dares)}",
        parse_mode=ParseMode.HTML,
    )


async def tod_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if random.random() > 0.5:
        truths = [
            "What's your biggest secret?",
            "Who do you have a crush on in this group?",
            "What's the most embarrassing thing you've ever done?",
        ]
        await update.message.reply_text(f"🎭 <b>Truth:</b> {random.choice(truths)}", parse_mode=ParseMode.HTML)
    else:
        dares = [
            "Send a voice message saying 'I love this group!'",
            "Write a short poem about the last person who messaged.",
            "Type the next message using only emojis.",
        ]
        await update.message.reply_text(f"🎭 <b>Dare:</b> {random.choice(dares)}", parse_mode=ParseMode.HTML)


async def ask_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Anonymous question to the group: /ask <question>"""
    if not context.args:
        return await update.message.reply_text("Usage: /ask <your anonymous question>")
    question = " ".join(context.args)
    try:
        await update.message.delete()
    except Exception:
        pass
    await context.bot.send_message(
        update.effective_chat.id,
        f"🔮 <b>Anonymous Question:</b>\n\n<i>{html.escape(question)}</i>",
        parse_mode=ParseMode.HTML,
    )


async def joke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    jokes = [
        ("Why don't scientists trust atoms?", "Because they make up everything!"),
        ("I asked my dog what two minus two is.", "He said nothing."),
        ("Why did the scarecrow win an award?", "Because he was outstanding in his field!"),
        ("I only know 25 letters of the alphabet.", "I don't know y."),
        ("Why can't you give Elsa a balloon?", "Because she'll let it go."),
        ("Did you hear about the mathematician who's afraid of negative numbers?", "He'll stop at nothing to avoid them."),
        ("Why do cows wear bells?", "Because their horns don't work."),
        ("I told my wife she was drawing her eyebrows too high.", "She looked surprised."),
        ("What's a skeleton's least favorite room?", "The living room."),
        ("Why don't eggs tell jokes?", "They'd crack each other up."),
    ]
    setup, punchline = random.choice(jokes)
    await update.message.reply_text(f"😄 {setup}\n\n👉 <i>{punchline}</i>", parse_mode=ParseMode.HTML)


async def meme_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    memes = [
        "This is fine 🔥",
        "Nobody:\nAbsolutely nobody:\nThis bot: sends memes at 3am",
        "Me: I'll sleep early tonight\nAlso me at 3am: /meme",
        "404: Meme not found. Have a blank canvas instead: □",
        "👁️👄👁️ It is what it is.",
        "Stonks 📈",
        "I am speed 🏃",
        "Hold up— wait a minute… something ain't right 🤔",
        "Friendship ended with sleeping schedule\nNow chaotic messaging is my best friend",
        "Me texting back 3 days later: 'just saw this, lol'",
    ]
    await update.message.reply_text(random.choice(memes))


async def fact_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    facts = [
        "Honey never spoils — archaeologists found 3,000-year-old honey in Egyptian tombs.",
        "A group of flamingos is called a flamboyance. 🦩",
        "Crows can recognize and remember human faces.",
        "Bananas are technically berries, but strawberries aren't.",
        "The Eiffel Tower grows about 6 inches taller in summer due to heat expansion.",
        "Cleopatra lived closer in time to the Moon landing than to the building of the Great Pyramid.",
        "Octopuses have three hearts and blue blood.",
        "A day on Venus is longer than a year on Venus.",
        "Wombats produce cube-shaped poo — the only animals known to do so.",
        "The longest recorded flight of a chicken is 13 seconds.",
        "Polar bear fur is actually transparent, not white — it reflects light.",
        "A bolt of lightning is five times hotter than the surface of the sun.",
        "The human body contains enough carbon to make about 9,000 pencils.",
        "Sharks are older than trees — they've been around for over 400 million years.",
    ]
    await update.message.reply_text(f"💡 <b>Fun Fact:</b>\n\n{random.choice(facts)}", parse_mode=ParseMode.HTML)


async def quote_text_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate a fake inspirational quote."""
    starters = [
        "The only limit is", "Believe in", "Success comes from",
        "Life is too short for", "Never stop", "Every day is a chance to",
        "Dream bigger than", "The secret to happiness is",
    ]
    middles = [
        "your imagination", "yourself", "hard work and passion",
        "negativity", "learning", "be better",
        "yesterday's dreams", "simply letting go",
    ]
    await update.message.reply_text(
        f"✨ <i>\"{random.choice(starters)} {random.choice(middles)}.\"</i>\n\n— Definitely Someone Famous",
        parse_mode=ParseMode.HTML,
    )


async def typerace_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sentences = [
        "The quick brown fox jumps over the lazy dog.",
        "All that glitters is not gold.",
        "Actions speak louder than words.",
        "A journey of a thousand miles begins with a single step.",
        "To be or not to be that is the question.",
        "In the beginning God created the heavens and the earth.",
        "It was the best of times it was the worst of times.",
    ]
    sentence = random.choice(sentences)
    chat_id  = update.effective_chat.id
    context.chat_data[f"typerace_{chat_id}"] = {
        "sentence": sentence,
        "started_at": time.time(),
    }
    await update.message.reply_text(
        f"⌨️ <b>Type Race!</b>\n\nType exactly:\n<code>{html.escape(sentence)}</code>",
        parse_mode=ParseMode.HTML,
    )


async def check_typerace(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or update.effective_chat.type == "private":
        return
    chat_id = update.effective_chat.id
    race_data = context.chat_data.get(f"typerace_{chat_id}")
    if not race_data:
        return
    if update.message.text.strip() == race_data["sentence"]:
        elapsed = time.time() - race_data["started_at"]
        context.chat_data.pop(f"typerace_{chat_id}", None)
        user = update.effective_user
        await update.message.reply_text(
            f"🏆 {mention(user)} won the type race in <b>{elapsed:.2f}s</b>!",
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════
#  GHOST MODE
# ══════════════════════════════════════════════════════════
@admin_only
async def ghost_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle ghost mode for the calling admin (bot deletes their commands silently)."""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    cursor.execute("SELECT 1 FROM ghost_mode WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    if cursor.fetchone():
        cursor.execute("DELETE FROM ghost_mode WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        conn.commit()
        await update.message.reply_text("👁️ Ghost mode <b>disabled</b>.", parse_mode=ParseMode.HTML)
    else:
        cursor.execute("INSERT INTO ghost_mode (chat_id, user_id) VALUES (?,?)", (chat_id, user_id))
        conn.commit()
        try:
            await update.message.delete()
        except Exception:
            pass


async def check_ghost_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete command messages from users in ghost mode."""
    if not update.message or not update.message.text:
        return
    if not update.message.text.startswith("/"):
        return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    cursor.execute("SELECT 1 FROM ghost_mode WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    if cursor.fetchone():
        try:
            await update.message.delete()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  MUTE ALL / UNMUTE ALL
# ══════════════════════════════════════════════════════════
@admin_only
async def muteall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restrict all non-admin members from sending messages."""
    chat_id = update.effective_chat.id
    try:
        await context.bot.set_chat_permissions(
            chat_id,
            ChatPermissions(
                can_send_messages=False,
                can_send_photos=False,
                can_send_videos=False,
                can_send_documents=False,
                can_send_polls=False,
                can_add_web_page_previews=False,
            ),
        )
        await update.message.reply_text("🔇 <b>All members muted!</b> Admins are unaffected.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def unmuteall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore all default permissions."""
    chat_id = update.effective_chat.id
    try:
        await context.bot.set_chat_permissions(
            chat_id,
            ChatPermissions(
                can_send_messages=True,
                can_send_photos=True,
                can_send_videos=True,
                can_send_documents=True,
                can_send_polls=True,
                can_add_web_page_previews=True,
                can_change_info=False,
                can_invite_users=True,
            ),
        )
        await update.message.reply_text("🔊 <b>All members unmuted!</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


# ══════════════════════════════════════════════════════════
#  USER PERSONAL NOTES
# ══════════════════════════════════════════════════════════
async def mynote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save a personal note: /mynote <key> <text> | /mynote <key> to retrieve."""
    args = context.args or []
    user_id = update.effective_user.id
    if not args:
        cursor.execute("SELECT note_key, content FROM user_notes WHERE owner_id=? LIMIT 20", (user_id,))
        rows = cursor.fetchall()
        if not rows:
            return await update.message.reply_text("📝 You have no personal notes. Use /mynote <key> <text> to save one.")
        lines = [f"• <code>{html.escape(k)}</code>: {html.escape(v[:50])}" for k, v in rows]
        return await update.message.reply_text(
            f"📝 <b>Your Notes:</b>\n\n" + "\n".join(lines),
            parse_mode=ParseMode.HTML,
        )
    key = args[0].lower()
    if len(args) == 1:
        cursor.execute("SELECT content FROM user_notes WHERE owner_id=? AND note_key=?", (user_id, key))
        row = cursor.fetchone()
        if row:
            return await update.message.reply_text(
                f"📝 <b>{html.escape(key)}:</b>\n\n{html.escape(row[0])}", parse_mode=ParseMode.HTML
            )
        return await update.message.reply_text(f"❌ Note <code>{html.escape(key)}</code> not found.", parse_mode=ParseMode.HTML)
    content = " ".join(args[1:])
    cursor.execute("INSERT OR REPLACE INTO user_notes (owner_id, note_key, content) VALUES (?,?,?)", (user_id, key, content))
    conn.commit()
    await update.message.reply_text(f"✅ Note <code>{html.escape(key)}</code> saved!", parse_mode=ParseMode.HTML)


async def delmynote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /delmynote <key>")
    key = context.args[0].lower()
    cursor.execute("DELETE FROM user_notes WHERE owner_id=? AND note_key=?", (update.effective_user.id, key))
    conn.commit()
    if cursor.rowcount:
        await update.message.reply_text(f"✅ Note <code>{html.escape(key)}</code> deleted.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ Note not found.")


# ══════════════════════════════════════════════════════════
#  MEMBER COUNT / PING / ECHO / MATH
# ══════════════════════════════════════════════════════════
async def membercount_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    count = await update.effective_chat.get_member_count()
    await update.message.reply_text(
        f"👥 <b>{html.escape(update.effective_chat.title or 'This group')}</b> has <b>{count}</b> members.",
        parse_mode=ParseMode.HTML,
    )


async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import time as _time
    start = _time.monotonic()
    msg = await update.message.reply_text("🏓 Pong!")
    elapsed = (_time.monotonic() - start) * 1000
    await msg.edit_text(f"🏓 Pong! <b>{elapsed:.1f} ms</b>", parse_mode=ParseMode.HTML)


async def echo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return
    if context.args:
        text = " ".join(context.args)
        try:
            await update.message.delete()
        except Exception:
            pass
        await context.bot.send_message(update.effective_chat.id, text)


async def calc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /calc <expression>  e.g. /calc 2 + 2 * 10")
    expr = " ".join(context.args)
    safe_expr = re.sub(r"[^0-9+\-*/().\s%]", "", expr)
    try:
        result = eval(safe_expr, {"__builtins__": {}})  # safe-ish for arithmetic only
        await update.message.reply_text(
            f"🧮 <code>{html.escape(expr)}</code> = <b>{result}</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        await update.message.reply_text("❌ Invalid expression.")


async def reverse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message and update.message.reply_to_message.text:
        text = update.message.reply_to_message.text
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Reply to a message or provide text: /reverse <text>")
    await update.message.reply_text(text[::-1])


async def mock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message and update.message.reply_to_message.text:
        text = update.message.reply_to_message.text
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Reply to or provide text: /mock <text>")
    mocked = "".join(c.upper() if i % 2 else c.lower() for i, c in enumerate(text))
    await update.message.reply_text(mocked)


async def clap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message and update.message.reply_to_message.text:
        text = update.message.reply_to_message.text
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Reply or: /clap <text>")
    await update.message.reply_text("👏".join(text.split()))


async def aesthetic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = " ".join(context.args) if context.args else ""
    if not text and update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    if not text:
        return await update.message.reply_text("Provide text: /aesthetic <text>")
    aes = " ".join(c for c in text)
    await update.message.reply_text(aes)


# ══════════════════════════════════════════════════════════
#  EXTRA INFO COMMANDS
# ══════════════════════════════════════════════════════════
async def mention_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get a clean mention link for a user."""
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    elif context.args:
        raw = context.args[0].lstrip("@")
        try:
            target = await context.bot.get_chat(int(raw) if raw.isdigit() else f"@{raw}")
        except Exception:
            return await update.message.reply_text("❌ User not found!")
    else:
        target = update.effective_user
    await update.message.reply_text(
        f"👤 Mention: {mention(target)}\n🔗 Link: <code>tg://user?id={target.id}</code>",
        parse_mode=ParseMode.HTML,
    )


async def userid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
        await update.message.reply_text(
            f"🆔 <b>{html.escape(target.first_name or '')}</b>: <code>{target.id}</code>",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"🆔 Your ID: <code>{update.effective_user.id}</code>",
            parse_mode=ParseMode.HTML,
        )


async def grouplink_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.username:
        link = f"https://t.me/{chat.username}"
    else:
        try:
            link_obj = await context.bot.export_chat_invite_link(chat.id)
            link = link_obj
        except Exception:
            link = "❌ Unable to get link (no permission?)"
    await update.message.reply_text(
        f"🔗 <b>Group Link:</b> {html.escape(link)}", parse_mode=ParseMode.HTML
    )


# ══════════════════════════════════════════════════════════
#  WARN ON JOIN (auto-check new members against gban)
# ══════════════════════════════════════════════════════════
async def welcome_and_checks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Umbrella handler for new members: captcha, anti-raid, gban check, welcome message."""
    if not update.message or not update.message.new_chat_members:
        return
    await check_anti_raid(update, context)
    await captcha_new_member(update, context)
    # Existing welcome logic is handled elsewhere; here we just trigger the checks


# ══════════════════════════════════════════════════════════
#  ANTIBOT COMMAND  (already in file, extended alias)
# ══════════════════════════════════════════════════════════
@admin_only
async def antibot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    if not context.args:
        cursor.execute("SELECT antibot FROM settings WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        st = "✅ On" if (row and row[0]) else "❌ Off"
        return await update.message.reply_text(
            f"🤖 <b>Anti-Bot</b>: {st}\nUsage: /antibot on|off",
            parse_mode=ParseMode.HTML,
        )
    sub = context.args[0].lower()
    val = 1 if sub == "on" else 0
    cursor.execute("UPDATE settings SET antibot=? WHERE chat_id=?", (val, chat_id))
    conn.commit()
    await update.message.reply_text(
        f"🤖 Anti-bot {'<b>enabled</b>' if val else '<b>disabled</b>'}! Bots joining will be kicked.",
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  EXTENDED WELCOME VARIABLES  (extra placeholders)
# ══════════════════════════════════════════════════════════
def format_welcome_extended(text: str, user, chat) -> str:
    """Replace extended placeholders in welcome/goodbye messages."""
    replacements = {
        "{first}":      html.escape(user.first_name or ""),
        "{last}":       html.escape(user.last_name or ""),
        "{fullname}":   html.escape(f"{user.first_name or ''} {user.last_name or ''}".strip()),
        "{username}":   f"@{user.username}" if user.username else html.escape(user.first_name or ""),
        "{mention}":    mention(user),
        "{id}":         str(user.id),
        "{chatname}":   html.escape(chat.title or ""),
        "{chatid}":     str(chat.id),
        "{count}":      "?",  # Would need an API call; placeholder
    }
    for placeholder, value in replacements.items():
        text = text.replace(placeholder, value)
    return text


# ══════════════════════════════════════════════════════════
#  MEDIA STATS
# ══════════════════════════════════════════════════════════
_media_counts: dict[int, dict] = {}   # chat_id -> {photo:N, video:N, sticker:N, ...}


async def track_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.effective_chat.type == "private":
        return
    chat_id = update.effective_chat.id
    _media_counts.setdefault(chat_id, {"photo": 0, "video": 0, "sticker": 0, "audio": 0, "document": 0, "voice": 0})
    msg = update.message
    if msg.photo:
        _media_counts[chat_id]["photo"] += 1
    elif msg.video or msg.video_note:
        _media_counts[chat_id]["video"] += 1
    elif msg.sticker:
        _media_counts[chat_id]["sticker"] += 1
    elif msg.audio:
        _media_counts[chat_id]["audio"] += 1
    elif msg.document:
        _media_counts[chat_id]["document"] += 1
    elif msg.voice:
        _media_counts[chat_id]["voice"] += 1


async def mediastats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    stats = _media_counts.get(chat_id, {})
    if not stats or all(v == 0 for v in stats.values()):
        return await update.message.reply_text("📊 No media tracked yet since I came online.")
    lines = [
        f"🖼️ Photos: <b>{stats.get('photo', 0)}</b>",
        f"🎬 Videos: <b>{stats.get('video', 0)}</b>",
        f"🎭 Stickers: <b>{stats.get('sticker', 0)}</b>",
        f"🎵 Audio: <b>{stats.get('audio', 0)}</b>",
        f"📄 Documents: <b>{stats.get('document', 0)}</b>",
        f"🎤 Voice: <b>{stats.get('voice', 0)}</b>",
    ]
    await update.message.reply_text(
        f"📊 <b>Media Stats (since bot started)</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  EXTRA MODERATION HELPERS
# ══════════════════════════════════════════════════════════
@admin_only
async def delmsg_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a message by ID: /delmsg <message_id>"""
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Usage: /delmsg <message_id>")
    mid = int(context.args[0])
    try:
        await context.bot.delete_message(update.effective_chat.id, mid)
        await update.message.reply_text(f"🗑️ Message {mid} deleted.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def kickme_force_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force-kick a user even without a target (for testing)."""
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ Can't kick an admin!")
    try:
        await context.bot.ban_chat_member(update.effective_chat.id, target.id)
        await context.bot.unban_chat_member(update.effective_chat.id, target.id)
        await update.message.reply_text(f"👢 {mention(target)} force-kicked!", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


@admin_only
async def banall_inactive_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban all members with 0 recorded messages in this group (careful!)."""
    chat_id = update.effective_chat.id
    if not context.args or context.args[0].lower() != "confirm":
        return await update.message.reply_text(
            "⚠️ This will ban all members with zero tracked messages!\n"
            "Type /baninactive confirm to proceed."
        )
    cursor.execute(
        "SELECT user_id FROM chat_members WHERE chat_id=? "
        "AND user_id NOT IN (SELECT user_id FROM group_msg_count WHERE chat_id=?)",
        (chat_id, chat_id),
    )
    rows = cursor.fetchall()
    banned = 0
    for (uid,) in rows:
        if await is_admin(update, context, uid):
            continue
        try:
            await context.bot.ban_chat_member(chat_id, uid)
            banned += 1
        except Exception:
            pass
    await update.message.reply_text(f"✅ Banned <b>{banned}</b> inactive members.", parse_mode=ParseMode.HTML)


@admin_only
async def warn_max_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Directly max-warn a user: /warnmax @user <reason>"""
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    if await is_admin(update, context, target.id):
        return await update.message.reply_text("⚠️ Can't warn an admin!")
    chat_id = update.effective_chat.id
    cursor.execute("SELECT max_warns, warn_action FROM settings WHERE chat_id=?", (chat_id,))
    s = cursor.fetchone()
    max_w, action = (s[0], s[1]) if s else (3, "ban")
    cursor.execute("INSERT OR REPLACE INTO warns (chat_id, user_id, count, reasons) VALUES (?,?,?,?)",
                   (chat_id, target.id, max_w, "Max-warned by admin"))
    conn.commit()
    await _warn_helper(context, chat_id, target, "Max-warned by admin")
    await update.message.reply_text(f"⚠️ {mention(target)} max-warned ({max_w}/{max_w}).", parse_mode=ParseMode.HTML)


@owner_only
async def sql_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run a read-only SQL query: /sql SELECT ..."""
    if not context.args:
        return await update.message.reply_text("Usage: /sql <SELECT query>")
    query = " ".join(context.args)
    if not query.strip().upper().startswith("SELECT"):
        return await update.message.reply_text("❌ Only SELECT queries allowed.")
    try:
        cursor.execute(query)
        rows = cursor.fetchmany(20)
        cols = [d[0] for d in cursor.description] if cursor.description else []
        if not rows:
            return await update.message.reply_text("✅ Query returned no rows.")
        header = " | ".join(cols)
        sep    = "-" * len(header)
        lines  = [header, sep] + [" | ".join(str(v) for v in r) for r in rows]
        await update.message.reply_text(
            f"<pre>{html.escape(chr(10).join(lines))}</pre>", parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await update.message.reply_text(f"❌ SQL error: {html.escape(str(e))}", parse_mode=ParseMode.HTML)


# ══════════════════════════════════════════════════════════
#  ANTI-SPAM SCORE TRACKER
# ══════════════════════════════════════════════════════════
_spam_score: dict[tuple, list] = {}   # (chat_id, user_id) -> [timestamps]
SPAM_SCORE_WINDOW  = 60    # seconds
SPAM_SCORE_MAX     = 20    # messages before flagged
SPAM_SIMILAR_RATIO = 0.85  # ratio of similarity to count as spam repeat
_last_msg: dict[tuple, str] = {}   # (chat_id, user_id) -> last message text


async def check_antispam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced anti-spam: rate + similarity check."""
    if not update.message or update.effective_chat.type == "private":
        return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if await is_admin(update, context, user_id):
        return
    cursor.execute("SELECT antispam FROM settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return
    now = time.time()
    key = (chat_id, user_id)
    _spam_score.setdefault(key, [])
    _spam_score[key] = [t for t in _spam_score[key] if now - t < SPAM_SCORE_WINDOW]
    _spam_score[key].append(now)
    text = (update.message.text or "").strip()
    last  = _last_msg.get(key, "")
    _last_msg[key] = text
    is_repeat = (
        text and last and
        text.lower() == last.lower() and
        len(_spam_score[key]) > 3
    )
    if len(_spam_score[key]) >= SPAM_SCORE_MAX or is_repeat:
        _spam_score[key] = []
        men = mention(update.effective_user)
        try:
            await context.bot.restrict_chat_member(
                chat_id, user_id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=datetime.now() + timedelta(minutes=10),
            )
            await context.bot.send_message(
                chat_id,
                f"🚫 {men} has been auto-muted for 10 minutes due to spam!",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  MODERATION LOG ENHANCEMENT
# ══════════════════════════════════════════════════════════
# setlog_cmd and unsetlog_cmd are defined earlier in the file


# ══════════════════════════════════════════════════════════
#  EXTENDED LOCK TYPES  (spoiler / code block / voice msg)
# ══════════════════════════════════════════════════════════
EXTENDED_LOCK_TYPES = ("spoiler", "code", "voicenote", "videonote", "dice", "game_share")


@admin_only
async def lockx_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Extended lock: /lockx <type>"""
    if not context.args or context.args[0].lower() not in EXTENDED_LOCK_TYPES:
        return await update.message.reply_text(
            f"🔒 Extended lock types: {', '.join(EXTENDED_LOCK_TYPES)}\nUsage: /lockx <type>"
        )
    ltype = context.args[0].lower()
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO settings (chat_id) VALUES (?)", (chat_id,))
    current_key = f"lock_{ltype}"
    cursor.execute(f"ALTER TABLE settings ADD COLUMN IF NOT EXISTS {current_key} INTEGER DEFAULT 0")
    cursor.execute(f"UPDATE settings SET {current_key}=1 WHERE chat_id=?", (chat_id,))
    conn.commit()
    await update.message.reply_text(f"🔒 <b>{ltype}</b> locked!", parse_mode=ParseMode.HTML)


@admin_only
async def unlockx_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0].lower() not in EXTENDED_LOCK_TYPES:
        return await update.message.reply_text(f"Usage: /unlockx <{'|'.join(EXTENDED_LOCK_TYPES)}>")
    ltype = context.args[0].lower()
    chat_id = update.effective_chat.id
    current_key = f"lock_{ltype}"
    try:
        cursor.execute(f"UPDATE settings SET {current_key}=0 WHERE chat_id=?", (chat_id,))
        conn.commit()
    except Exception:
        pass
    await update.message.reply_text(f"🔓 <b>{ltype}</b> unlocked!", parse_mode=ParseMode.HTML)


async def check_extended_locks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.effective_chat.type == "private":
        return
    user_id = update.effective_user.id
    if await is_admin(update, context, user_id):
        return
    chat_id = update.effective_chat.id
    msg = update.message
    deleted = False

    # Check dice lock
    try:
        cursor.execute("SELECT lock_dice FROM settings WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        if row and row[0] and msg.dice:
            await msg.delete()
            deleted = True
    except Exception:
        pass

    # Check video note lock
    try:
        cursor.execute("SELECT lock_videonote FROM settings WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        if row and row[0] and msg.video_note:
            await msg.delete()
            deleted = True
    except Exception:
        pass

    # Check spoiler (entities)
    try:
        cursor.execute("SELECT lock_spoiler FROM settings WHERE chat_id=?", (chat_id,))
        row = cursor.fetchone()
        if row and row[0]:
            entities = msg.entities or msg.caption_entities or []
            if any(e.type == "spoiler" for e in entities):
                await msg.delete()
                deleted = True
    except Exception:
        pass


# ══════════════════════════════════════════════════════════
#  FORCE UNMUTE ALL ON COMMAND
# ══════════════════════════════════════════════════════════
@admin_only
async def unmuteuser_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unmute a specific user by ID without replying: /unmuteuser <user_id>"""
    if not context.args or not context.args[0].lstrip("-").isdigit():
        return await update.message.reply_text("Usage: /unmuteuser <user_id>")
    uid = int(context.args[0])
    chat_id = update.effective_chat.id
    try:
        await context.bot.restrict_chat_member(
            chat_id, uid,
            permissions=ChatPermissions(
                can_send_messages=True, can_send_photos=True,
                can_send_videos=True, can_send_documents=True,
            ),
        )
        await update.message.reply_text(f"🔊 User <code>{uid}</code> unmuted.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def unbanuser_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unban by user ID: /unbanuser <user_id>"""
    if not context.args or not context.args[0].lstrip("-").isdigit():
        return await update.message.reply_text("Usage: /unbanuser <user_id>")
    uid = int(context.args[0])
    chat_id = update.effective_chat.id
    try:
        await context.bot.unban_chat_member(chat_id, uid)
        await update.message.reply_text(f"✅ User <code>{uid}</code> unbanned.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


# ══════════════════════════════════════════════════════════
#  HELP EXTENDED (topic-based help)
# ══════════════════════════════════════════════════════════
HELP_TOPICS = {
    "moderation": (
        "🔨 <b>Moderation Commands</b>\n\n"
        "/ban — Ban a user\n/unban — Unban a user\n/kick — Kick a user\n"
        "/mute — Mute a user\n/unmute — Unmute a user\n/tmute — Temp-mute with roast\n"
        "/tban — Temp-ban\n/warn — Warn a user\n/warns — Check warns\n"
        "/unwarn — Remove a warn\n/resetwarns — Reset all warns\n"
        "/promote — Promote to admin\n/demote — Demote admin\n"
        "/purge — Purge messages\n/del — Delete a message\n"
        "/warnmax — Instantly max-warn\n/muteall — Mute everyone\n/unmuteall — Unmute everyone"
    ),
    "locks": (
        "🔒 <b>Locks</b>\n\n"
        "/lock <type> — Lock a content type\n/unlock <type> — Unlock\n/locks — Show all locks\n"
        "/lockx <type> — Extended locks (spoiler/code/dice/videonote)\n"
        "/capsfilter — Filter CAPS messages\n/emojifilter — Filter emoji spam\n"
        "/antiforward — Block forwarded messages\n"
        "Lock types: sticker, link, forward, photo, video, document, audio, voice, gif, poll, text, all"
    ),
    "filters": (
        "🔍 <b>Filters & Notes</b>\n\n"
        "/filter <kw> <resp> — Add keyword filter\n/stop <kw> — Remove filter\n/filters — List filters\n"
        "/save <name> <text> — Save note\n/get <name> — Get note\n/clear <name> — Delete note\n"
        "/notes — List notes\n#note_name — Retrieve note by hashtag"
    ),
    "welcome": (
        "👋 <b>Welcome & Rules</b>\n\n"
        "/setwelcome — Set welcome message\n/setgoodbye — Set goodbye message\n"
        "/welcome — Toggle welcome on/off\n/goodbye — Toggle goodbye\n"
        "/cleanwelcome — Auto-delete previous welcome\n"
        "/rules — Show rules\n/setrules — Set rules\n/clearrules — Delete rules\n\n"
        "<b>Placeholders:</b> {mention} {first} {last} {fullname} {username} {id} {chatname}"
    ),
    "captcha": (
        "🔒 <b>Captcha & Verification</b>\n\n"
        "/captcha on|off — Enable/disable captcha for new members\n"
        "/captcha button — Button-click challenge\n/captcha math — Math challenge\n"
        "/captcha timeout <secs> — Set timeout\n"
        "/forcesub @channel — Require channel subscription\n"
        "/forcesub off — Disable force-sub\n"
        "/antiraid on|off — Anti-raid protection\n/antiraid set <thr> <window> <action>"
    ),
    "fun": (
        "🎮 <b>Fun Commands</b>\n\n"
        "/coinflip — Heads or tails\n/dice [sides] — Roll a die\n"
        "/8ball — Magic 8-ball\n/rps rock|paper|scissors — Play RPS\n"
        "/roast @user — Roast someone\n/compliment @user — Compliment\n"
        "/hug @user — Hug\n/slap @user — Slap\n/pat @user — Pat\n"
        "/ship @user — Love compatibility\n/love @user — Love meter\n"
        "/trivia — Answer a trivia question\n/triviascore — Leaderboard\n"
        "/roll [NdM] — Dice roll\n/choose a|b|c — Random choice\n"
        "/rate @user — Rate someone\n/iq @user — IQ check\n/gay @user — Gay meter\n"
        "/pp @user — Measure...\n/joke — Random joke\n/fact — Random fact\n"
        "/truth — Truth question\n/dare — Dare challenge\n/tod — Truth or Dare\n"
        "/ask <question> — Anonymous question\n/meme — Random meme text\n"
        "/reverse <text> — Reverse text\n/mock <text> — sPoNgEbOb\n"
        "/clap <text> — 👏 Clap 👏 text\n/aesthetic <text> — A e s t h e t i c\n"
        "/calc <expr> — Calculator\n/typerace — Typing race game"
    ),
    "karma": (
        "⭐ <b>Karma System</b>\n\n"
        "Reply +1, thanks, 👏 to give karma\nReply -1, 👎 to take karma\n"
        "/karma @user — Check karma\n/ktop — Karma leaderboard\n/kresetall — Reset all karma (admins)"
    ),
    "stats": (
        "📊 <b>Statistics</b>\n\n"
        "/topactive — Most active members\n/msgcount @user — Message count\n"
        "/mediastats — Media type breakdown\n/membercount — Group size\n"
        "/stats — Bot session stats\n/botstats — Owner: full bot stats\n/chatinfo — Group info"
    ),
    "admin": (
        "🛡️ <b>Admin Tools</b>\n\n"
        "/setlog <channel_id|here> — Set log channel\n/unsetlog — Remove log\n"
        "/addcmd <cmd> <resp> — Add custom command\n/rmcmd <cmd> — Remove custom command\n"
        "/cmds — List custom commands\n/backup — Backup group settings\n"
        "/restore — Restore from backup\n/schedule <time> <text> — Schedule a message\n"
        "/cancelschedule <id> — Cancel scheduled message\n/autodelete <secs|off> — Auto-delete timer\n"
        "/slowmode <secs|off> — Set slowmode\n/ghost — Toggle ghost mode\n"
        "/antibot on|off — Block bots from joining\n/sql <SELECT> — Run SQL query (owner)\n"
        "/echo <text> — Send text anonymously (admin)"
    ),
}


async def help_topic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help for a specific topic: /help <topic>"""
    if not context.args:
        topics_list = "\n".join(f"• /help {t}" for t in HELP_TOPICS)
        return await update.message.reply_text(
            f"📚 <b>Help Topics:</b>\n\n{topics_list}\n\nOr just /help for the main menu.",
            parse_mode=ParseMode.HTML,
        )
    topic = context.args[0].lower()
    if topic in HELP_TOPICS:
        await update.message.reply_text(HELP_TOPICS[topic], parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"❓ Unknown topic <code>{html.escape(topic)}</code>.\nAvailable: {', '.join(HELP_TOPICS.keys())}",
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════
#  REPORT LOG COMMAND
# ══════════════════════════════════════════════════════════
async def reportlog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return
    chat_id = update.effective_chat.id
    cursor.execute(
        "SELECT reporter_id, reported_id, reason, reported_at FROM report_log "
        "WHERE chat_id=? ORDER BY reported_at DESC LIMIT 10",
        (chat_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("📋 No reports logged for this group.")
    lines = []
    for rep_by, rep_uid, reason, ts in rows:
        dt = datetime.fromtimestamp(ts).strftime("%m/%d %H:%M")
        lines.append(f"• By <code>{rep_by}</code> → <code>{rep_uid}</code>: <i>{html.escape(reason[:40])}</i> [{dt}]")
    await update.message.reply_text(
        f"📋 <b>Recent Reports:</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
#  MASTER MESSAGE HANDLER
# ══════════════════════════════════════════════════════════
async def master_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    # Auto-cache user on every message — writes to MongoDB first, then SQLite
    cache_user(update.effective_user)

    # Track the group and record per-group membership — MongoDB primary
    if update.effective_chat.type != "private":
        track_group(update.effective_chat)
        record_chat_member(update.effective_chat.id, update.effective_user)

    # Global ban enforcement
    if update.effective_chat.type != "private":
        gban_doc = is_gbanned(update.effective_user.id)
        if gban_doc:
            try:
                await context.bot.ban_chat_member(
                    update.effective_chat.id, update.effective_user.id
                )
                men = mention(update.effective_user)
                await context.bot.send_message(
                    update.effective_chat.id,
                    f"⚠️ Globally banned user {men} was auto-removed!\n"
                    f"Reason: <i>{html.escape(gban_doc.get('reason', ''))}</i>",
                    parse_mode=ParseMode.HTML,
                )
            except (BadRequest, Forbidden):
                pass
            return

    # Check for @all trigger
    text = update.message.text or update.message.caption or ""
    if "@all" in text.lower() and update.effective_chat.type != "private":
        await handle_tag_all(update, context)

    # ── Group-only passive checks ───────────────────────────────────────
    if update.effective_chat.type != "private":
        await check_force_sub(update, context)
        await track_message_count(update, context)
        await track_media(update, context)
        await check_karma(update, context)
        await handle_custom_cmd(update, context)
        await check_typerace(update, context)
        await check_trivia(update, context)
        await check_anti_forward(update, context)
        await check_caps_filter(update, context)
        await check_emoji_filter(update, context)
        await check_antispam(update, context)
        await check_extended_locks(update, context)
        await check_ghost_mode(update, context)
        await check_auto_delete(update, context)

    await check_afk(update, context)
    await check_blacklist(update, context)
    await enforce_locks(update, context)
    await enforce_admin_locks(update, context)
    await check_flood(update, context)
    await process_filters(update, context)
    await check_hashtag_note(update, context)




# ══════════════════════════════════════════════════════════════════════════════
#  EXTENDED FEATURES BLOCK  —  The Manager v2.0
#  Fun · Games · Utilities · Missing Rose Features · Text Manipulation
# ══════════════════════════════════════════════════════════════════════════════

JOKES = [
    "Why don't scientists trust atoms? Because they make up everything!",
    "I told my wife she was drawing her eyebrows too high. She looked surprised.",
    "Why did the scarecrow win an award? Because he was outstanding in his field!",
    "I'm reading a book about anti-gravity. It's impossible to put down!",
    "Did you hear about the mathematician who's afraid of negative numbers? He'll stop at nothing to avoid them!",
    "Why can't you give Elsa a balloon? Because she'll let it go!",
    "What do you call a fake noodle? An impasta!",
    "Why did the bicycle fall over? Because it was two-tired!",
    "What do you call a fish without eyes? A fsh!",
    "Why do cows wear bells? Because their horns don't work!",
    "What did the ocean say to the beach? Nothing, it just waved!",
    "Why don't eggs tell jokes? They'd crack each other up!",
    "I used to hate facial hair but then it grew on me.",
    "What do you call a man with no body and no nose? Nobody knows!",
    "Why do we tell actors to 'break a leg?' Because every play has a cast!",
    "I'm on a seafood diet. I see food and I eat it.",
    "What do you call cheese that isn't yours? Nacho cheese!",
    "Why did the golfer bring extra socks? In case he got a hole in one!",
    "What do you call a sleeping dinosaur? A dino-snore!",
    "I asked the librarian if they had books about paranoia. She whispered 'they're right behind you!'",
    "Why do bananas wear sunscreen? Because they peel!",
    "What do you call a bear with no teeth? A gummy bear!",
    "I would tell you a construction joke but I'm still working on it.",
    "What do you call a snowman with a six-pack? An abdominal snowman!",
    "Why did the coffee file a police report? It got mugged!",
    "What do you call a lazy kangaroo? A pouch potato!",
    "Why did the stadium get hot after the game? All the fans left!",
    "What do you call a boomerang that won't come back? A stick!",
    "I'm afraid for the calendar. Its days are numbered.",
    "What do you call a can opener that doesn't work? A can't opener!",
    "Why did the scarecrow become a successful politician? Because he was outstanding in his field and full of hot air!",
    "What do you call a sad cup of coffee? A depresso!",
    "Did you hear about the claustrophobic astronaut? He just needed a little space!",
    "What do you call a pony with a cough? A little hoarse!",
    "I don't trust stairs because they're always up to something.",
    "What do you get when you cross a snowman and a vampire? Frostbite!",
    "Why do seagulls fly over the ocean? Because if they flew over the bay, they'd be bagels!",
    "I tried to catch fog earlier. I mist.",
    "What do you call a sleeping triceratops? A dino-snore with three horns!",
    "I asked my dog what two minus two is. He said nothing.",
    "Why did the picture go to jail? Because it was framed!",
    "What do you call a parade of rabbits hopping backwards? A receding hare-line!",
    "Why don't some couples go to the gym? Because some relationships don't work out!",
    "I have a lot of growing up to do. I realized that the other day inside my fort.",
    "What do you call an alligator in a vest? An investigator!",
    "Why don't ants get sick? Because they have little anty-bodies!",
    "What do you call a factory that makes okay products? A satisfactory!",
    "I don't have a carbon footprint. I just drive everywhere.",
    "Why did the invisible man turn down the job offer? He couldn't see himself doing it!",
    "What do you call a magic dog? A labracadabrador!",
    "I used to play piano by ear, but now I use my hands.",
    "What do you call a hippo that believes in peace? A hippiecrite!",
    "Why did the math book look so sad? Because it had too many problems!",
    "What did one wall say to the other wall? I'll meet you at the corner!",
    "Why can't Cinderella play soccer? Because she always runs away from the ball!",
    "What do you call a fish that wears a crown? A king fish!",
    "I'm not lazy, I'm on energy-saving mode.",
    "Why did the tomato turn red? Because it saw the salad dressing!",
    "What do you call a man who can't stand? Neil!",
    "Why did the belt go to jail? For holding up some pants!",
    "What do you get from a pampered cow? Spoiled milk!",
    "Why did the nurse bring a red pen to work? In case she needed to draw blood!",
    "What did the left eye say to the right eye? Between you and me, something smells.",
    "Why did the scarecrow become a motivational speaker? Because he was outstanding in his field of influence!",
    "What do you call a bee that can't make up its mind? A maybe!",
    "I tried writing with a broken pencil once. It was pointless.",
    "What do you call a funny mountain? Hill-arious!",
    "Why did the golfer wear two pairs of pants? In case he got a hole in one!",
    "What do you call a number that can't keep still? A roamin' numeral!",
    "I'm reading a great book about teleportation. It'll take you places.",
    "What do you call a ghost's true love? His ghoul-friend!",
    "Why don't skeletons fight each other? They don't have the guts!",
    "What do you call a pig that does karate? A pork chop!",
    "Why did the music teacher need a ladder? To reach the high notes!",
    "I asked my cat if she wanted to hear a joke. She said 'meh' so I told her anyway.",
    "Why did the physics teacher break up with the biology teacher? There was no chemistry!",
    "What do you call a cow with no legs? Ground beef!",
    "What do you call a cow with two legs? Lean beef!",
    "I'm great at multitasking. I can waste time, be unproductive, and procrastinate all at once.",
    "What do you call a sleeping bull? A bulldozer!",
    "Why did the pie go to the dentist? Because it needed a filling!",
    "What do you call a shoe made from a banana? A slipper!",
    "Why did the computer go to the doctor? It had a virus!",
    "I told a joke about paper. It was tearable.",
    "What do you call a bee that lives in America? A USB!",
    "Why are elevator jokes so good? They work on so many levels!",
    "I have a joke about construction but I'm still building up to it.",
    "What did the janitor say when he jumped out of the closet? Supplies!",
    "What do you call a penguin in the desert? Lost!",
    "Why did the banana go to the doctor? It wasn't peeling well!",
    "What do you call an elephant that doesn't matter? An irrelephant!",
    "I used to be a banker, but I lost interest.",
    "What do you call it when a snowman throws a temper tantrum? A meltdown!",
    "Why don't penguins like talking to strangers at parties? Because they find it hard to break the ice!",
    "What do you call a belt with a watch on it? A waist of time!",
    "I'm on a whiskey diet. I've lost three days already.",
    "What do you call a rabbit who is really cool? A hip-hop-otamus!",
    "Why did the teddy bear say no to dessert? Because she was already stuffed!",
    "What did the judge say to the dentist? Do you swear to pull the tooth, the whole tooth?",
    "I used to be a banker. Then I lost interest in the whole thing.",
    "Why did the can crusher quit his job? It was soda pressing!",
    "Why did the orange fail math? It couldn't concentrate!",
    "I asked my dad for his best dad joke. He said, 'You.'",
    "Why did the coach go to the bank? To get his quarterback!",
    "I don't trust staircases. They're always up to something.",
    "What do you call a sad strawberry? A blueberry!",
    "Why was the math book stressed? It had too many unsolved problems.",
    "What do you call two birds in love? Tweethearts!",
    "I tried to sue the airport for losing my luggage. I lost my case.",
    "Why was the broom late? It swept in!",
    "What do you call a fake stone in Ireland? A sham-rock!",
    "I have a joke about chemistry but I know I wouldn't get a reaction.",
    "What do you call a bear without any ears? B!",
    "Why did the invisible man turn down the job? He couldn't see himself doing it.",
    "I'm reading a horror story in Braille. Something bad is about to happen, I can feel it.",
    "What do you call cheese that's standing by itself? Provolone!",
    "Why was the calendar in therapy? Too many dates!",
    "What do you call a pile of cats? A meow-ntain!",
    "I'm making a documentary about clocks. It's about time.",
    "What do you call a short psychic on the run? A small medium at large!",
    "What do you call a fish that needs help with their vocals? Auto-tuna!",
    "I asked a Frenchman if he played video games. He said Wii.",
    "Why are ghosts such bad liars? Because you can see right through them!",
    "What do you call a blind dinosaur? Do-ya-think-he-saurus!",
    "I got hit in the head with a can of diet soda. I'm okay — it was a soft drink.",
    "Why did the nurse go to art school? To learn how to draw blood.",
    "What do you call a gorilla wearing ear muffs? Anything you want, he can't hear you!",
    "I'm reading a book on the history of gravity. I can't put it down.",
    "What do you call a cow on a trampoline? A milkshake!",
    "Why did the scientist install knockers on his door? He wanted to win the No-bell prize!",
    "I got a new pair of gloves, but they're both left-handed. On the other hand, that's not right.",
    "What do you call a fly with no wings? A walk!",
    "Why don't oysters share? Because they're shellfish!",
    "What do you call a computer that sings? A Dell!",
    "I'm a social media influencer. I influence people to put down their phones.",
    "Why was the broom late for work? It overswept!",
    "Why do cows have hooves instead of feet? Because they lactose!",
    "I'm reading a book about mazes. I keep getting lost in it.",
    "Why was the ocean so salty? The land never waves back!",
    "I just got fired from my job at the bakery. They said I was loafing around.",
    "Why did the dinosaur cross the road? Because chickens didn't exist yet!",
    "I wrote a book about birds. It has a lot of tweets in it.",
    "Why did the coach go to the bank? To get his quarterback back!",
    "I used to be a professional poker player, but then I had a bad hand.",
    "Why did the golfer wear two watches? Because he had a lot of time on his hands!",
    "I have a joke about pizza but it's too cheesy.",
    "Why did the duck get arrested? For selling quack medicines!",
    "I stayed up all night wondering where the sun went. Then it dawned on me!",
    "Why was the candle burnt out? It was burning out!",
    "What do you call a snake that works for the government? A civil serpent!",
    "I bought some shoes from a drug dealer. I don't know what he laced them with, but I was tripping all day.",
    "Why was the detective bad at math? He had too many unsolved cases!",
    "I have a joke about infinity. It doesn't have an end.",
    "Why are pirates called pirates? Because they arrr!",
    "I'm great at sleeping. I can do it with my eyes closed.",
    "I have a lot of jokes about unemployed people, but none of them work.",
    "Why did the math teacher divide by zero? He wanted to see what the fuss was about!",
    "What do you call a duck that gets all A's? A wise quacker!",
    "I tried to catch some fog earlier. I mist.",
    "I have a terrific joke about paper. Never mind, it's tearable.",
    "Why did the tree go to the barber? For a little trim!",
    "I used to hate beards, but they grew on me.",
    "Why was the school in chaos? The pen was mightier than the sword and the teacher lost the pen!",
    "I've decided to sell my vacuum cleaner — it was just collecting dust.",
    "Why can't Elsa have a balloon? Because she'll let it go!",
    "I asked the librarian if they had books about paranoia. She whispered, 'Yes, and they know you're here.'",
    "Why was the skeleton always calm? Because nothing got under his skin!",
    "I have a joke about paper. I'll tear myself away from it.",
    "What do you call a horse that moves around a lot? Unstable!",
    "Why was the astronaut so bad at relationships? He needed too much space!",
    "I used to work at a calendar factory but I got fired for taking a couple days off.",
    "I'm reading a great book about mountains. It has a lot of high points.",
    "I told my doctor I had a problem with my memory. He told me to forget about it.",
    "Why did the chemistry book look sad? Too many compounds and not enough solutions!",
    "What do you call a skeleton who just woke up? Bone-tired!",
    "What do you call it when a dinosaur gets in a car crash? Tyrannosaurus wrecks!",
    "Why did the owl start a podcast? He was a real hoot!",
    "What do you call a lazy bee? A drone!",
    "I bought the world's worst thesaurus. Not only is it terrible, it's terrible.",
    "Why are there no basketball courts in Africa? Because of all the cheetahs!",
    "I was wondering why the Frisbee was getting bigger. Then it hit me.",
    "Why did the pencil win the race? Because it had a sharp start!",
    "I'm reading a book about wind. It blows me away.",
    "Why was the computer always tired? It had too many Windows open!",
    "I made a pun about vegetables but I'm too carrot to share it.",
    "Why was the train always stressed? It had too many passengers riding on it!",
    "I wanted to tell a chemistry joke but all the good ones Argon.",
    "Why did the melon jump into the lake? It wanted to be a water-melon!",
    "What do you call a French man wearing sandals? Philippe Flop!",
    "I'm on a seafood diet — whenever I see food, I eat it. Works great at buffets.",
    "What do you call a frog that's stuck in traffic? Toad-ally annoyed!",
    "I can't believe I got fired from the calendar factory. All I did was take a day off!",
    "What do you call a ghost's favorite type of road? A dead end!",
    "I bought myself a new dictionary but when I got home, all the pages were blank. I have no words for how angry I am.",
    "Why was the baseball stadium so cool? It had a fan in every seat!",
    "I have a photographic memory but I always forget to charge it.",
    "Why was the math teacher's garden so beautiful? She had square roots!",
    "I'm writing a book about submarines. It's a novel under the sea.",
    "Why did the sun go to school? To get brighter!",
    "I have a great pun about electricity but it's too shocking.",
    "Why was the swimming pool so popular? It had a lot of depth!",
    "I was going to share a joke about vegetables but I decided not to, I didn't want to start any beefs.",
    "Why was the clock always sad? Its hands were always pointing to work!",
    "What do you call a group of unicorns? A blessing!",
    "I have a joke about wifi. Never mind, you might not connect with it.",
    "I've been trying to come up with a good egg joke but I can't seem to crack one.",
    "Why did the student eat his homework? Because the teacher told him it was a piece of cake!",
    "I used to be an astronaut, but I got fired. I took up too much space.",
    "Why was the balloon so nervous? It was afraid it would pop under pressure!",
    "I tried to explain to my kids that it's okay to watch TV all day. The TV is now broken.",
    "Why was the guitar so loud? Because it had too many strings attached!",
    "I have a joke about amnesia. I forgot the punchline.",
    "Why did the lamp fail his exams? He wasn't bright enough!",
    "What do you call a very formal spider? A tuxedo weaver!",
    "I'm writing a book about hurricanes. It's a real whirlwind read.",
    "What do you call a duck with a briefcase? A business duck!",
    "What do you call a bee that never gives up? Deter-minate!",
    "Why was the robot angry? People kept pushing his buttons!",
    "I was going to tell an astronomy joke but the one I thought of was too far out.",
    "What do you call a snobbish prisoner? A condescending con!",
    "I have a joke about running but I'm still working on it.",
    "I'm reading a book on the history of glue — can't put it down.",
    "What do you call a dinosaur that takes care of its teeth? A floss-iraptor!",
    "What do you call a witch who lives at the beach? A sand-witch!",
    "I'm writing a book on reverse psychology. Please don't buy it.",
    "What do you call a cold dog? A pupsicle!",
    "I didn't want to believe my dad was stealing from his job as a traffic cop, but when I came home, all the signs were there.",
    "What do you call a cheerful road? A merry-way!",
    "What do you call a number scared of seven? Six!",
    "I have a joke about procrastination. I'll tell you later.",
    "I'm not sarcastic. I'm fluent in 'I can't believe you just said that.'",
    "My GPS told me to turn around. I said no. Now I'm lost.",
    "I tried to take a hot air balloon ride. The pilot said it was too on the nose.",
    "Why do programmers prefer dark mode? Because light attracts bugs!",
    "A programmer's wife says 'go to the store and get a loaf of bread, and if they have eggs, get a dozen.' He came back with 12 loaves of bread.",
    "Why do programmers hate nature? It has too many bugs and no documentation.",
    "I told my doctor I broke my arm in two places. He told me to stop going to those places.",
    "Why are frogs so happy? They eat whatever bugs them!",
    "What do you call a sleeping Egyptian? A mummy!",
    "What do you call a pile of kittens? A meowtain of cuteness!",
    "I'm organizing a space-themed party. I need to planet.",
    "Why was the cat sitting on the computer? To keep an eye on the mouse!",
    "What do you call an alligator who's a detective? An investi-gator!",
    "I stayed up all night to see where the sun went. Then it dawned on me.",
    "What do you call cheese all by itself at a party? A lone-ly cheese!",
    "What do you call a very careful wolf? A cautious wolf-pack leader!",
    "What do you call a cat that gets everything it wants? Purrr-suasive!",
    "I used to be addicted to the hokey pokey, but I turned myself around.",
    "What do you call a very tiny insect? A mini-bug!",
    "Why was the music teacher locked out of his classroom? His keys were in the piano!",
    "What do you call a happy cowboy? Jolly Rancher!",
    "What do you call a man with a plank on his head? Edward!",
    "Why did the chicken visit the therapist? To get to the other side of its issues!",
    "What do you call a very small singer? A microphone!",
    "What do you call a sleeping baker? A roll model!",
    "What do you call a snobbish criminal going down stairs? A condescending con, descending!",
    "What do you call a book club that's been stuck on the same book for years? A waste of thyme!",
    "What do you call a factory making just alright products? A satisfactory!",
    "What do you call a cloud with an attitude? Nimbo-stratus!",
    "Why did the ocean break up with the pond? It thought the pond was too shallow.",
    "What do you call a very small valentine? A valen-tiny!",
    "I'm so good at sleeping I can do it with my eyes closed.",
    "My wife told me I had to stop acting like a flamingo. I had to put my foot down.",
    "I can't take my dog to the park because the ducks keep trying to bite him. I guess that's what I get for buying a pure bread dog.",
    "I have a joke about chemistry but I know I wouldn't get a reaction. So I'm not telling it.",
    "Why was the math book unhappy? It had too many problems.",
    "Did you hear about the fire at the circus? It was in tents!",
    "What do you call a naughty lamb dressed up like a skeleton for halloween? Mutton dressed as boo!",
    "Why did the bank robber take a bath? Because he wanted to make a clean getaway.",
    "What's a scarecrow's favorite fruit? Straw-berries.",
    "I accidentally swallowed some Scrabble tiles. My next trip to the bathroom could spell disaster.",
    "Why did the math student eat his homework? The teacher told him it was a piece of cake.",
    "I used to think I was indecisive, but now I'm not so sure.",
    "What do you call a man who's always late? Justin.",
    "Why can't you trust an atom? Because they make up literally everything!",
    "I'm reading a book called 'The History of Glue' — I just can't seem to put it down.",
    "What do you call a very fast zombie? A zoombie!",
    "I went to a bookstore and asked where the self-help section was. The guy said if he told me it would defeat the purpose.",
    "What do you call a fish in a tuxedo? So-fish-ticated!",
    "Why did the toilet paper roll down the hill? To get to the bottom!",
    "What do you call a man lying on your doorstep? Mat!",
    "What's a vampire's favorite fruit? A blood orange!",
    "Why do bees have sticky hair? Because they use honeycomb!",
    "What do you call a dog that can do magic? A labra-cadabra-dor!",
    "I asked the gym instructor if he could teach me to do the splits. He said how flexible are you? I said I can't make Tuesdays.",
    "What do you call a T-Rex who lost their keys? Tyrannosaurus wrecks!",
    "Why did the mushroom go to the party alone? Because he was a fungi!",
    "I told my friend that I was reading a book about helium. He said 'HeHe'.",
    "What do you call a Frenchman wearing sandals? Phillipe Phloppe.",
    "Why did the soccer player bring string to the game? So he could tie the score!",
    "What's the difference between a good joke and a bad joke timing.",
    "I'm reading a mystery novel about a man who becomes obsessed with finding the perfect pen. It's a gripping tale.",
    "What do you call an optimistic letter of the alphabet? A positive B!",
    "My wife said I needed to grow up. I was speechless. It's hard to say anything when you have 60 gummy bears in your mouth.",
    "What do you call a sleeping pizza? A snore-dough!",
    "Why did the vegetable win the award? Because it was out-standing in its field! (Yes, even plants can be scarecrows.)",
    "Why can't you give Elsa a balloon? Because she'll let it go! (Again, still true.)",
    "I told my son he was adopted. He said 'That's not true, is it?' I said 'Well you certainly didn't come with good looks from me!'",
    "What do you call a group of parrots? A company! What do you call a group of crows? A murder. Parrots sound way more fun.",
    "I used to be afraid of hurdles, but I got over it.",
    "Why was the broom late to work? It swept through the night!",
]

FACTS = [
    "Honey never spoils — archaeologists have found 3,000-year-old honey in Egyptian tombs that was still perfectly good.",
    "A day on Venus is longer than a year on Venus. It takes Venus 243 Earth days to rotate once, but only 225 Earth days to orbit the Sun.",
    "The shortest war in history was between Britain and Zanzibar in 1896. Zanzibar surrendered after 38 to 45 minutes.",
    "A group of flamingos is called a flamboyance.",
    "Cleopatra lived closer in time to the Moon landing than to the construction of the Great Pyramid of Giza.",
    "Octopuses have three hearts, blue blood, and can change both color and texture of their skin.",
    "The Eiffel Tower can be 15 cm taller during the summer due to thermal expansion of the metal.",
    "A jiffy is an actual unit of time: 1/100th of a second.",
    "There are more possible iterations of a game of chess than there are atoms in the observable universe.",
    "Crows can recognize human faces and hold grudges for years.",
    "The average person walks about 100,000 miles in a lifetime, enough to circle the Earth four times.",
    "Sharks are older than trees — sharks have existed for over 400 million years, while trees appeared about 350 million years ago.",
    "A snail can sleep for three years.",
    "The fingerprints of a koala are virtually indistinguishable from those of a human, even under a microscope.",
    "Bananas are technically berries, but strawberries are not.",
    "The Statue of Liberty was originally intended to be placed at the Suez Canal in Egypt.",
    "Wombats produce cube-shaped poop. They are the only animals known to do this.",
    "A group of owls is called a parliament.",
    "The ocean produces over 50% of the world's oxygen, mostly from microscopic marine plants.",
    "Humans share about 60% of their DNA with bananas.",
    "Light takes about 8 minutes and 20 seconds to travel from the Sun to Earth.",
    "The Great Wall of China is not visible from space with the naked eye, despite the popular myth.",
    "Almonds are a member of the peach family.",
    "The oldest known living organism is a bristlecone pine tree in California called Methuselah, estimated to be over 5,000 years old.",
    "An octopus has the ability to edit its own RNA, potentially allowing it to adapt to its environment in real time.",
    "The word 'muscle' comes from the Latin word 'musculus' meaning 'little mouse' because a flexing muscle looks like a mouse moving under skin.",
    "Elephants are the only animals that can't jump.",
    "A blue whale's heart is so large that a human could crawl through its arteries.",
    "The average human body contains enough iron to make a 3-inch nail.",
    "There are more trees on Earth than stars in the Milky Way galaxy.",
    "The dot above a lowercase 'i' is called a tittle.",
    "A group of pandas is called an embarrassment.",
    "Butterflies taste with their feet.",
    "It is impossible to hum while holding your nose closed.",
    "A shrimp's heart is in its head.",
    "It takes a photon about 100,000 years to travel from the Sun's core to its surface, but only 8 minutes to Earth.",
    "Cats have 32 muscles in each ear.",
    "The Hawaiian alphabet only has 13 letters.",
    "A 'butt' is actually a unit of measurement for wine and equals about 477 liters.",
    "Male seahorses are the ones that get pregnant and give birth.",
    "A group of crows is called a murder.",
    "Scotland's national animal is the unicorn.",
    "The longest English word without a vowel is 'rhythms'.",
    "A day on Mercury lasts longer than a year on Mercury.",
    "Turtles can breathe through their butts.",
    "All the ants on Earth weigh roughly the same as all the humans on Earth.",
    "Hot water freezes faster than cold water in some conditions — this is called the Mpemba effect.",
    "A group of kittens is called a kindle.",
    "The first computer bug was an actual bug — a moth found in a computer relay at Harvard in 1947.",
    "Starfish don't have a brain or blood. They use seawater to pump nutrients through their system.",
    "The total weight of all the bacteria in the human body roughly equals the weight of the human brain.",
    "Dogs' nose prints are as unique as human fingerprints.",
    "A cloud can weigh more than a million pounds.",
    "The human eye can distinguish about 10 million different colors.",
    "Penguins propose to their mates with a pebble.",
    "The letter 'Q' is the only letter that doesn't appear in any US state name.",
    "Mosquitoes are attracted to people who have recently eaten bananas.",
    "Cows have best friends and become stressed when separated from them.",
    "The Earth is not a perfect sphere — it bulges slightly at the equator.",
    "The skin is the largest organ of the human body.",
    "Lightning strikes the Earth about 100 times per second.",
    "The Amazon rainforest produces about 20% of the world's oxygen.",
    "Dolphins sleep with one eye open.",
    "The average person has about 70,000 thoughts per day.",
    "A group of jellyfish is called a smack.",
    "The Mona Lisa has no eyebrows or eyelashes.",
    "A group of hippos is called a bloat.",
    "The longest recorded flight of a chicken is 13 seconds.",
    "Men's shirts have buttons on the right; women's shirts have buttons on the left.",
    "A group of butterflies is called a kaleidoscope.",
    "An average person laughs about 15 times per day.",
    "The letter 'e' is the most commonly used letter in the English language.",
    "Cats have whiskers on the backs of their front legs too.",
    "The Empire State Building was built in just 410 days.",
    "Honey bees can recognize human faces.",
    "A group of baboons is called a congress.",
    "The average person blinks about 15-20 times per minute.",
    "A group of geese on the ground is called a gaggle; in the air, a skein.",
    "Grasshoppers have five eyes — two compound eyes and three simple eyes.",
    "The heart of a blue whale beats only about 2 times per minute.",
    "The human brain generates about 12-25 watts of electricity when awake.",
    "Giraffes have the same number of vertebrae in their necks as humans — seven.",
    "A group of ferrets is called a business.",
    "Apples belong to the rose family, as do pears and plums.",
    "The word 'nerd' was first coined by Dr. Seuss in 'If I Ran the Zoo' in 1950.",
    "Grapes explode when microwaved.",
    "Cats can make over 100 different vocal sounds; dogs can make about 10.",
    "Every single atom in your body is billions of years old.",
    "The ocean floor has more ancient artifacts than all the world's museums combined.",
    "A human's pinky finger contributes about 50% of hand strength.",
    "Pineapples take approximately 18 to 24 months to grow.",
    "The fingerprints of identical twins are different.",
    "Alfred Hitchcock's film Psycho was the first American film to show a toilet flushing.",
    "A group of ravens is called a conspiracy.",
    "Sand from the Sahara is occasionally blown all the way to the Amazon rainforest, fertilizing it.",
    "An average tree has about 200,000 leaves.",
    "Right-handed people tend to chew food on the right side of their mouth.",
    "The average person produces enough saliva in a lifetime to fill two swimming pools.",
    "Humans are the only animals that cry emotional tears.",
    "Pistachios are members of the cashew family.",
    "There are more than 7,500 varieties of apples grown around the world.",
    "Male lions sleep up to 20 hours a day.",
    "The strongest muscle in the human body is the masseter, or jaw muscle.",
    "A group of cats is called a clowder.",
    "The first fax machine was invented in 1843, before the telephone.",
    "Sea otters hold hands while sleeping so they don't drift apart.",
    "The average person spends about 6 years of their life dreaming.",
    "Crows are one of the few species known to use tools.",
    "Antarctica is the only continent without a time zone.",
    "Butterflies can see ultraviolet light.",
    "The moon is slowly drifting away from Earth at about 3.8 cm per year.",
    "The word 'pizza' was first recorded in a Latin text from a town in Italy in 997 AD.",
    "Penguins can jump up to 9 feet in the air.",
    "A human baby has about 300 bones at birth, which fuse to 206 by adulthood.",
    "Owls don't have eyeballs — they have eye tubes that can't move in their sockets.",
    "An elephant's pregnancy lasts about 22 months.",
    "The human nose can detect over 1 trillion different smells.",
    "Polar bears are left-pawed.",
    "A group of sharks is called a shiver.",
    "The first Olympic games were held in 776 BC in Olympia, Greece.",
    "The Amazon River discharges more water than any other river on Earth.",
    "A group of pugs is called a grumble.",
    "Lobsters were once considered the cockroaches of the sea and were fed to prisoners.",
    "The human heart beats about 100,000 times per day.",
    "Some turtles can live for over 150 years.",
    "The first email ever sent was by Ray Tomlinson to himself in 1971.",
    "A group of vultures is called a committee.",
    "Bees have five eyes.",
    "Crocodiles cannot stick out their tongue.",
    "An ostrich's eye is bigger than its brain.",
    "A group of lizards is called a lounge.",
    "The first words spoken on the moon were 'The Eagle has landed.'",
    "Tigers have striped skin, not just striped fur.",
    "A group of penguins in water is called a raft; on land, a waddle.",
    "A snail can have up to 14,000 teeth.",
    "Elephants can communicate through seismic vibrations in the ground.",
    "Humans are the only animals that blush.",
    "A group of bears is called a sloth.",
    "Whale song can be heard from hundreds of miles away underwater.",
    "Gorillas share 98.3% of their DNA with humans.",
    "A group of rhinos is called a crash.",
    "The word 'goodbye' comes from 'God be with ye'.",
    "A group of iguanas is called a mess.",
    "The average human scalp has 100,000 hair follicles.",
    "A cockroach can live for a week without its head before dying of thirst.",
    "The first domain name ever registered was Symbolics.com in 1985.",
    "Humans and dogs share about 84% of the same DNA.",
    "A group of narwhals is called a blessing.",
    "The word 'lunatic' comes from luna, the Moon, because people thought the Moon caused madness.",
    "Octopuses have three hearts and nine brains (one central and one in each arm).",
    "The smallest country in the world is Vatican City at about 44 hectares.",
    "In Japan, letting a sumo wrestler make your baby cry is considered good luck.",
    "A group of herons is called a siege.",
    "There are over 3,500 different species of mosquitoes in the world.",
    "The average human sneezes at around 160 km/h.",
    "A group of hyenas is called a cackle.",
    "Babies are born with more bones than adults — 300 vs 206.",
    "A group of kangaroos is called a mob.",
    "The oldest language still in use today is Tamil, which is about 5,000 years old.",
    "A group of giraffes is called a tower.",
    "The water we drink today is the same water dinosaurs drank millions of years ago.",
    "A group of wolves is called a pack.",
    "The first movie ever made with sound was 'The Jazz Singer' in 1927.",
    "A group of squid is called a squad.",
    "Sloths move so slowly that algae actually grows on their fur.",
    "The sun is so large that about 1.3 million Earths could fit inside it.",
    "A group of cockroaches is called an intrusion.",
    "Your brain uses 20% of all the oxygen and calories your body consumes.",
    "A group of meerkats is called a mob or a gang.",
    "The shortest complete sentence in the English language is 'Go.'",
    "A group of lions is called a pride.",
    "A group of kangaroos in a mob can have over 50 members.",
    "You cannot sneeze with your eyes open.",
    "A group of flamingos is called a stand or flamboyance.",
    "There are more grains of sand on Earth's beaches than stars in the observable universe.",
    "There are more possible unique chess games than atoms in the observable universe.",
    "A group of cats is called a clowder, but kittens are called a kindle.",
    "The global average of internet usage is about 7 hours per day per person.",
    "Male ducks are called drakes, females are called ducks.",
    "A group of fish moving together is called a school.",
]

COMPLIMENTS = [
    "You're more wonderful than a double rainbow on a rainy day! 🌈",
    "Your smile could power a small country for a week!",
    "You make the world more beautiful just by existing in it!",
    "You're the human equivalent of a warm hug and hot cocoa.",
    "You're not just a ray of sunshine, you're the whole entire sun!",
    "The world is genuinely a better place because you're in it.",
    "You have the energy of someone who just found a $20 bill in an old jacket.",
    "Your laugh is genuinely contagious — in the best possible way!",
    "You're the kind of person legends are written about.",
    "You could make a rainy Monday feel like a sunny Saturday.",
    "You're absolutely incredible and don't let anyone tell you otherwise.",
    "Everything you touch turns to gold — metaphorically, but still impressive.",
    "You're basically a superhero, but in a cooler, more subtle way.",
    "Your brain is like Google, but friendlier and less invasive.",
    "You have a heart made of solid gold, but lighter and warmer.",
    "You could make a cactus feel comfortable.",
    "Your energy is basically renewable — you power up everyone around you.",
    "If kindness were currency, you'd be a billionaire.",
    "You're the plot twist that every story needs.",
    "You're so amazing that even mirrors do a double-take.",
    "Your presence makes every room about 300% more interesting.",
    "You're basically the human embodiment of a standing ovation.",
    "You make excellence look effortless.",
    "You're not just smart — you're the kind of smart that changes things.",
    "The universe took its absolute best shot when it made you.",
    "You're a limited edition — there's genuinely no one else like you.",
    "Your positive energy could charge a dead phone.",
    "You're the reason someone's day went from bad to great.",
    "You carry yourself with the confidence of someone who knows exactly who they are.",
    "Talking to you is always the highlight of someone's day.",
    "You have the rare gift of making people feel truly seen and heard.",
    "You approach everything with such grace and intelligence.",
    "You're the real MVP in every situation you're in.",
    "Your creativity is absolutely out of this world.",
    "You bring joy wherever you go — sometimes even before you arrive.",
    "You're the type of person who makes others want to be better.",
    "Your dedication is seriously inspiring.",
    "You've got that special combination of smart AND kind, which is incredibly rare.",
    "You're basically walking proof that good people exist.",
    "Your potential is absolutely limitless.",
    "You make difficult things look easy, and easy things look amazing.",
    "You're the kind of friend everyone wishes they had.",
    "Your integrity is one of your most underrated superpowers.",
    "You handle challenges with such remarkable grace.",
    "You've got the kind of charisma that fills a room.",
    "You're genuinely one of the most thoughtful people around.",
    "Your ambition is so refreshing and inspiring.",
    "You have an incredible ability to see the best in everyone.",
    "You're a masterpiece that's still in progress — and already stunning.",
    "You make everything you're a part of better, always.",
    "Your resilience is genuinely remarkable.",
    "You're the kind of person people talk about in a good way.",
    "Your empathy is your secret superpower.",
    "You deserve every good thing that comes your way — and then some.",
    "You're shining so bright it's hard to look directly at you.",
    "You're basically proof that awesome people exist.",
    "Your insight is consistently impressive.",
    "You always know exactly what to say to make things better.",
    "You have such amazing energy that even plants perk up when you enter.",
    "You're a rare combination of talented, kind, and funny.",
    "You're the definition of what it means to be truly human.",
    "Your generosity is one of the most beautiful things about you.",
    "You're a role model whether you know it or not.",
    "You have impeccable taste in everything.",
    "You're sharper than a fresh set of pencils.",
    "Your presence is genuinely a gift to everyone around you.",
    "You've got the kind of warmth that makes winter feel optional.",
    "You think of others in ways most people don't even think of themselves.",
    "You've got game — in every sense of the word.",
    "You are genuinely one of a kind and the world knows it.",
    "You have a magical way of turning problems into solutions.",
    "You're so full of life it's honestly inspiring.",
    "You're the most wonderfully weird person — in the best possible way.",
    "Your confidence is earned and totally justified.",
    "You've got the spirit of a champion and the heart of a saint.",
    "You're not just talented — you're gifted.",
    "You're so good at being you that it seems effortless.",
    "Your dedication to the things you care about is incredibly moving.",
    "You bring something irreplaceable to every group you're in.",
    "You're that person who makes everyone in the room feel special.",
    "You have an extraordinary ability to stay grounded while dreaming big.",
    "Everything you create has your unique fingerprints — and it's beautiful.",
    "You're so well-rounded it's almost unfair to everyone else.",
    "Your sense of humor is like a healing salve for difficult days.",
    "You're genuinely the best part of many people's stories.",
    "You're a lighthouse — steady, bright, and guiding people home.",
    "You're one of those rare people who actually makes the world better just by being here.",
    "You have the kind of intelligence that doesn't brag, it just quietly solves things.",
    "Your character is your greatest achievement.",
    "You're effortlessly magnetic — people just naturally want to be around you.",
    "You're not just a good person, you're a great person.",
    "You've got the kind of vibe that could defrost even the coldest personalities.",
    "You're magnificent in ways that don't fit in a compliment.",
    "You're so much more than the sum of your parts.",
    "You're the kind of person who makes other people feel brave.",
    "Your wisdom is way beyond your years.",
    "You're so thoughtful it sometimes feels like you're reading minds.",
    "You're brilliant at the most important things.",
    "You're proof that kindness is a form of genius.",
    "Your light is one of the brightest in the room, always.",
    "You're a walking reminder that good things do happen.",
    "You have the ability to make even the most mundane things interesting.",
    "You're exactly the person this world needs more of.",
    "Your honesty is refreshing and your loyalty is rare.",
]

ROASTS_EXTENDED = [
    "I'd explain it to you but I left my crayons at home.",
    "I've been called worse things by better people.",
    "Your wifi name probably is something like 'Loading...'.",
    "You have the energy of someone who drinks decaf ironically.",
    "Your vibe is a participation trophy.",
    "If you were any less sharp, you'd be a circle.",
    "You are the human equivalent of a mild inconvenience.",
    "Your life story is a deleted scene.",
    "Somewhere out there a tree is tirelessly producing oxygen for you. Apologize to it.",
    "You're the type who brings a spoon to a knife fight.",
    "If brains were gasoline, you wouldn't have enough to fuel a go-kart around a Cheerio.",
    "You're not the dumbest person on Earth, but you better hope they don't die.",
    "Your selfies look like you're trying to commit a crime and warn the victims at the same time.",
    "You peaked at 'hello'.",
    "There is literally no one who could describe you as a breath of fresh air.",
    "You set low standards and then fail to meet them.",
    "Being with you is like being at a buffet where everything is beige.",
    "You remind me of a cloud. When you disappear, it's a beautiful day.",
    "You're everything I like to avoid in a person.",
    "You must have a very large back pocket — because that's where you keep your personality.",
    "You're proof that even evolution can make mistakes occasionally.",
    "I'm not saying you're dumb, but you'd fail a Rorschach test if you got shown a hamburger.",
    "Your common sense is in airplane mode.",
    "You're the reason they put instructions on shampoo bottles.",
    "If being wrong were an Olympic sport, you'd lose on a technicality.",
    "Your personality runs like Windows 95 — slow and crashes at the worst moments.",
    "You have the decision-making skills of a Magic 8-Ball.",
    "You're about as useful as a chocolate teapot.",
    "Your confidence and your competence are inversely proportional.",
    "You're everyone's last call, not first choice.",
    "If you were a vegetable, you'd be a turnip — nobody really wants you at dinner.",
    "You speak fluent nonsense but struggle with common sense.",
    "Your filter runs on a really bad algorithm.",
    "You bring absolutely nothing to the table, except maybe the table itself.",
    "I'm not saying you're a bad person, but your personality is on do-not-disturb.",
    "You're the type who misses the forest AND the trees.",
    "Looking at your choices in life is like watching a slow-motion train derailment.",
    "You are living proof that quantity is not quality.",
    "Even your shadow needs a break from you sometimes.",
    "Your resume has way too much white space.",
    "Your ambitions and your achievements share essentially no overlap.",
    "You're the human equivalent of a Terms & Conditions page nobody reads.",
    "You make people around you cherish their alone time.",
    "Your mental load is basically a carry-on that got lost at baggage claim.",
    "You're the 'unsubscribe from all' button in human form.",
    "Your elevator does go to the top, it just refuses to.",
    "You've got two speeds: wrong and very wrong.",
    "You're like a movie that's 40 minutes too long and has no plot.",
    "Your sense of direction is lost in itself.",
    "I've seen better decisions made by vending machines.",
    "You're not infallible, you just have a very robust denial system.",
    "Your energy is 'leftover birthday cake at the office' — forced and oddly sad.",
    "You'd lose a staring contest with a sunset.",
    "You have the listening skills of a rock, but rocks are more interesting.",
    "Your spontaneity is deeply planned and still disappointing.",
    "You're a walking spoiler for the plot twist nobody asked for.",
    "Your opinions land like a wet newspaper on a cold morning.",
    "You argue in a language only you speak, which explains a lot.",
    "You're not a red flag, you're the whole matador cosplay.",
    "You're the only person who can turn a compliment into a liability.",
    "I don't always understand you, but when I do, I wish I didn't.",
    "Your presence makes people check their calendars to see if they can reschedule.",
    "You make silence seem deeply underrated.",
    "If there were an award for not reading the room, you'd somehow not hear the announcement.",
    "You're a walking contradiction who argues with their own earlier points.",
    "Your sense of humor is still loading.",
    "You're the main character in a show nobody watches.",
    "You bring the 'awkward' to every situation free of charge.",
    "Your best quality is how much you make people appreciate other people.",
    "You'd criticize a rainbow for being too colorful.",
    "Your self-awareness took a wrong turn and hasn't found its way back.",
    "Your ego wrote a check your personality couldn't cash.",
    "You're fine in the way that a smoke alarm is fine — mostly off and occasionally shrieking.",
    "You'd second-guess a weather forecast on a cloudless day.",
    "You're the 'terms have changed' popup that won't go away.",
    "You solve no problems and create several new ones.",
    "You are the plot hole in your own life story.",
    "Your vibe is strictly 'technical difficulties'.",
    "You're a cautionary tale wrapped in a humble brag.",
    "You're so predictable that even surprises see you coming.",
    "You talk in circles in a perfectly square room.",
    "Your confidence is a fascinating study in misplaced certainty.",
    "You've got the emotional range of a loading screen.",
    "You leave every conversation with more questions than you arrived with.",
    "Your personality has the energy of a phone at 3%.",
    "You're not wrong often, but when you are, you're spectacularly wrong.",
    "You're the reason there's a disclaimer on everything.",
    "You describe yourself better in third person — both are inaccurate.",
    "You're everyone's last resort and you somehow take that as a compliment.",
    "Your plot armor is transparently thin.",
    "If overthinking were exercise, you'd actually be in shape.",
    "You peaked so early you've essentially been on a gap year for years.",
    "You're a one-person argument with yourself that neither side wins.",
    "You're not the worst option — you're just not the first ten.",
    "Your autobiography would be filed under Fiction.",
    "You've got big energy for someone with very little receipts.",
    "You're so self-impressed for someone who hasn't given anyone else reason to be.",
    "You're the 404 error page of social situations.",
    "Your whole attitude screams 'I didn't read the room but I'll lecture it'.",
    "You're the backup plan that plans to never get called.",
    "You're not just out of touch, you're in a different timezone.",
    "Your takes age like milk in the sun.",
    "You're a cautionary tale with good lighting.",
    "Your main contribution to any conversation is proving that quantity isn't quality.",
]

TRUTHS = [
    "What's the most embarrassing thing you've ever done in public?",
    "Have you ever lied to get out of a social event?",
    "What's the worst gift you've ever received and pretended to like?",
    "Have you ever blamed something on someone else to avoid getting in trouble?",
    "What's your most irrational fear?",
    "What's the most childish thing you still do?",
    "Have you ever stalked someone's social media for hours?",
    "What's the biggest lie you've ever told?",
    "Have you ever pretended to be sick to avoid responsibilities?",
    "What's your most embarrassing autocorrect fail?",
    "What's something you pretend to like but secretly dislike?",
    "What's the most ridiculous thing you've cried about?",
    "Have you ever walked into a room and completely forgotten why?",
    "What's the longest you've ever gone without showering?",
    "Have you ever laughed at the wrong moment and made things awkward?",
    "What's your biggest guilty pleasure?",
    "Have you ever secretly eaten someone else's food?",
    "What's a weird habit you have when you're alone?",
    "Have you ever pretended not to see someone in public?",
    "What's the most embarrassing thing in your search history right now?",
    "Have you ever talked about someone and then that person appeared?",
    "What's something you've done that you've never told anyone?",
    "Have you ever laughed at something you weren't supposed to?",
    "What's your most embarrassing childhood memory?",
    "Have you ever said something awful behind someone's back?",
    "What's the worst decision you've made in the past year?",
    "Have you ever drunk-texted someone embarrassing things?",
    "What's the most ridiculous argument you've ever had?",
    "Have you ever pretended to know someone you didn't?",
    "What's the most embarrassing thing your parents have caught you doing?",
    "Have you ever ghosted someone and regretted it?",
    "What's something you're ashamed of but find funny now?",
    "Have you ever sent a message to the wrong person and panicked?",
    "What's the pettiest thing you've ever done?",
    "Have you ever pretended to laugh when you didn't get a joke?",
    "What would your parents be most disappointed to find out about?",
    "Have you ever tripped and acted like nothing happened?",
    "What's the most awkward thing you've said to a crush?",
    "Have you ever snuck out when you were supposed to be home?",
    "What's the most embarrassing thing on your phone right now?",
    "Have you ever said 'you too' when a waiter said 'enjoy your meal'?",
    "Have you ever eavesdropped on a conversation and heard something shocking?",
    "What's the worst advice you've ever given and someone actually followed?",
    "Have you ever pretended to be asleep to avoid a conversation?",
    "What's a secret talent you have that you've never told anyone?",
    "Have you ever copied someone else's homework or work?",
    "What's the most ridiculous thing you've ever purchased?",
    "Have you ever broken something and blamed it on someone else?",
    "What's the cringiest thing you ever posted online?",
    "Have you ever lied about your age?",
    "What's your most embarrassing nickname and who gave it to you?",
    "Have you ever cried at a movie you expected to be terrible?",
    "What's the weirdest thing you've ever done alone at home?",
    "Have you ever eaten food that fell on the floor?",
    "What's the most passive-aggressive thing you've ever done?",
    "Have you ever rejected someone and immediately regretted it?",
    "What's the most embarrassing thing you've done on a date?",
    "Have you ever laughed until something came out of your nose?",
    "What's the longest you've worn the same outfit without washing it?",
    "Have you ever worn mismatched shoes in public by accident?",
    "What's the most embarrassing voice message you've sent?",
    "Have you ever made up an excuse and the lie got completely out of hand?",
    "What's the dumbest thing you've done while sleep-deprived?",
    "Have you ever accidentally liked a super old post while stalking someone?",
    "What's something you bought specifically to impress someone?",
    "Have you ever walked into a glass door or wall?",
    "What's the weirdest food combination you actually enjoy?",
    "Have you ever pretended to be on the phone to avoid someone?",
    "What's something you're way too competitive about?",
    "Have you ever faked being interested in something to impress someone?",
    "What's the most embarrassing thing you've been caught doing?",
    "Have you ever said 'I love you' and immediately regretted it?",
    "What's a lie on your resume or social media that's still there?",
    "Have you ever cried at a commercial?",
    "What's something tiny that makes you irrationally angry?",
    "Have you ever fallen asleep during a movie you claimed to love?",
    "What's the most dramatic exit you've ever made from a situation?",
    "Have you ever started drama that got way bigger than intended?",
    "What's the most embarrassing thing you've done for attention?",
    "Have you ever pretended to understand something you completely didn't?",
    "Have you ever talked to a pet for way longer than you'd admit?",
    "What's a compliment you received that you didn't deserve at all?",
    "Have you ever walked out of a store and then remembered you forgot to pay?",
    "What's something you've Googled that you're embarrassed about?",
    "Have you ever called a teacher 'mom' or 'dad' by accident?",
    "Have you ever tried to text one person and sent it to a group chat?",
    "What's the most embarrassing nickname for yourself on your own phone?",
    "Have you ever laughed at a funeral?",
    "What's the most cringe thing in your camera roll right now?",
    "Have you ever been so wrong about something you argued confidently about?",
    "Have you ever faked an accent and kept it going too long?",
    "What's something you did in middle school that still haunts you?",
    "Have you ever lied about being stuck in traffic when you just didn't want to go?",
    "What's something you know way too much about for no real reason?",
    "Have you ever bought a book just for the aesthetic and never read it?",
    "What's the last thing you Googled that you'd never say out loud?",
    "Have you ever rehearsed a conversation in your head and then completely bombed it in real life?",
    "What's the most embarrassing reason you've stayed in a bad situation?",
    "Have you ever pretended to enjoy a food just to seem more adventurous?",
    "What is the most embarrassing thing that has happened to you in front of a large crowd?",
    "Have you ever pretended to be busy to avoid talking to someone?",
    "What's something you still believe that most people would call ridiculous?",
    "Have you ever cheated at a game and got caught?",
    "What is the strangest dream you've had that you remember?",
    "Have you ever had a crush on someone you probably shouldn't have?",
    "What's the most embarrassing thing you've done in front of your parents?",
    "Have you ever spread a rumor that wasn't true?",
    "What's the worst haircut you've ever had and what were you thinking?",
]

DARES = [
    "Do your best impression of a famous person for 1 minute.",
    "Send a voice message to someone in this chat singing 'Happy Birthday' off-key.",
    "Change your profile picture to a ridiculous one for 1 hour.",
    "Text someone 'I know what you did' without any further explanation.",
    "Do 20 jumping jacks in your current location right now.",
    "Share the most embarrassing photo on your phone in this chat.",
    "Call someone and talk to them in a fake foreign accent for 2 minutes.",
    "Write a dramatic love poem about an everyday object in your room.",
    "Do the worm dance and send a video.",
    "Text your most recent contact 'Oops, wrong number! Forget you saw that.'",
    "Change your display name to something ridiculous for the rest of the day.",
    "Say the alphabet backwards as fast as you can.",
    "Put ice cubes in your shirt for 30 seconds.",
    "Do your best celebrity impression and post a voice note.",
    "Speak in rhymes for the next 5 messages in this chat.",
    "Text someone 'Can you keep a secret?' and then send nothing for 10 minutes.",
    "Like the 10 most recent posts of someone you haven't interacted with.",
    "Send a selfie making the worst possible face you can.",
    "Let someone in this chat post one thing on your social media.",
    "Text your mom or dad a random fact about ants.",
    "Do your best impression of a movie trailer narrator.",
    "Post a picture of your feet (socks included) in the chat.",
    "Type your full name using only your elbows.",
    "Go 10 minutes without using any vowels in your messages.",
    "Text someone from your contacts 'Did you feel that?' and respond mysteriously.",
    "Speak like a Shakespearean character for the next 5 minutes.",
    "Spell your name using celebrity names.",
    "Do 15 push-ups right now and send evidence.",
    "Set a weird alarm title and post a screenshot.",
    "Call a random food delivery place and ask if they deliver 'vibes'.",
    "Let the person to your left (or next message sender) rename you in the chat.",
    "Send an extremely long text message to a contact saying nothing important.",
    "Take a selfie with the weirdest thing in your room.",
    "Talk in a whisper voice for the next 10 minutes while in a call.",
    "Pretend you're giving a TED talk about your pet (or imaginary pet) for 2 minutes.",
    "Draw your favorite animal with your non-dominant hand and post it.",
    "Order something at a restaurant in a medieval knight voice.",
    "Reply to every message for 10 minutes starting with 'My liege,'.",
    "Send a voice note describing your room like it's a crime scene.",
    "Go to your chat list and send 'thinking of you' to 5 random people.",
    "Do a freestyle rap about this group chat — minimum 4 lines.",
    "Make up a new word, define it, and use it in 3 sentences.",
    "Announce your most embarrassing moment as if reading the news.",
    "Let someone else pick your contact name for the next hour.",
    "Call a friend and speak only in questions for the entire conversation.",
    "Eat a spoonful of something you dislike and post the reaction.",
    "Text your 5th contact something completely random and screenshot the response.",
    "Do an interpretive dance of your morning routine and send a video.",
    "Write a fake Yelp review of your own house.",
    "Make a new group chat with 3 contacts called 'Do Not Join This'.",
    "Speak in a bad Australian accent for the next 10 messages.",
    "Recite pi to 10 decimal places from memory (or try to).",
    "Text someone 'I've been hiding something from you' and then respond with 'I eat apples and pretend they're chips'.",
    "Tell a 60-second story using only sound effects in a voice note.",
    "Do 10 squats while humming your national anthem.",
    "Write a haiku about the last thing you ate.",
    "Change your phone language to a language you don't speak for 5 minutes.",
    "Send a formal resignation letter to a group chat you're in.",
    "Give a dramatic weather report for your current room.",
    "Post a status update describing what you're doing right now in the most dramatic possible way.",
    "Text someone 'The package has been delivered. Await further instructions.'",
    "Make up a conspiracy theory about something mundane, like bread.",
    "Describe your day as if narrating an action movie.",
    "Do your best infomercial voice for a random object near you.",
    "Challenge someone in this chat to a staring contest via video call.",
    "Text someone 'Nice try' with no context.",
    "Post a video of you talking about why you're the main character.",
    "Pretend you're on a cooking show and describe making a sandwich dramatically.",
    "Text three people 'I still think about that day' with no context.",
    "Do 30 seconds of dramatic crying with no explanation.",
    "Sing your name to the tune of a popular song.",
    "Write a strongly-worded letter to your ceiling.",
    "Describe the last movie you watched as badly as possible.",
    "Text your best friend 'We need to talk' and then send 'never mind, all good'.",
    "Do a fashion show with things from your closet in the next 5 minutes.",
    "Use only 5-syllable words in your next 3 messages.",
    "Pretend to be a robot for the next 5 messages.",
    "Send a voice message where you narrate everything around you like a documentary.",
    "Text someone 'Just so you know, I accept your apology' with no context.",
    "Send a selfie pretending to be surprised at nothing.",
    "Post a voice note of you counting backwards from 100 at high speed.",
    "Order someone else to do a dare — but you have to do it too.",
    "Type your next 5 messages entirely in capital letters with extra exclamation marks!!!!",
    "Narrate the last 5 minutes of your life in third person.",
    "Give your phone to someone else for 2 minutes and let them do what they want.",
    "Write an apology letter to your 10-year-old self.",
    "Make a list of your top 3 enemies (real or imaginary) and share it.",
    "Do a 60-second motivational speech about getting out of bed.",
    "End every message for the next 15 minutes with 'per my last email'.",
    "Send a voice message in the most dramatic storytelling voice of something boring.",
    "Text someone 'I just got back from the future. We need to talk.'",
    "Do your best James Bond entrance into your current room.",
    "Answer the next 5 questions as if you're in a job interview.",
    "Pretend to be from the middle ages for the next 3 minutes.",
    "Reenact the last movie scene you watched with household objects.",
    "Send a series of 5 photos telling a completely silent story.",
    "Text your best contact '911 what's your emergency' with no other context.",
    "Make the most dramatic 'I've been betrayed' face and send a selfie.",
    "Do 3 minutes of stand-up comedy right now in a voice note.",
    "Teach everyone in the chat a useless skill via voice message.",
    "Text someone 'I forgive you' with zero context.",
    "Pretend you just won an Oscar and give an acceptance speech.",
    "Read the last 5 messages in the chat in a dramatic documentary narrator voice.",
    "Say 'banana' in every message for the next 10 minutes.",
    "Challenge the next person who messages to a rap battle.",
    "Do your best impression of a news anchor reporting on something silly.",
]

WOULD_YOU_RATHER = [
    "Would you rather be able to fly or be invisible?",
    "Would you rather have unlimited money or unlimited time?",
    "Would you rather live in a world without music or without movies?",
    "Would you rather always be 10 minutes early or always be 20 minutes late?",
    "Would you rather only be able to whisper or only be able to shout?",
    "Would you rather have the power to read minds or the power to predict the future?",
    "Would you rather be an amazing singer or an incredible athlete?",
    "Would you rather live without your phone for a year or without internet for a year?",
    "Would you rather know how you'll die or when you'll die?",
    "Would you rather be the funniest person in the room or the smartest?",
    "Would you rather only eat sweet foods or only eat savory foods forever?",
    "Would you rather be able to speak all languages or play all instruments?",
    "Would you rather be famous for something embarrassing or unknown forever?",
    "Would you rather never feel pain or never feel hungry?",
    "Would you rather be the hero or the villain who everyone finds fascinating?",
    "Would you rather explore outer space or the deep ocean?",
    "Would you rather have a personal chef or a personal driver?",
    "Would you rather lose all memories from birth to 18 or all memories from the last 3 years?",
    "Would you rather have the ability to stop time or travel through time?",
    "Would you rather always know someone is lying or always know when they're honest?",
    "Would you rather be able to teleport anywhere or have super speed?",
    "Would you rather live in a fantasy world or a sci-fi world?",
    "Would you rather have all the money you'll ever need or find your perfect soulmate?",
    "Would you rather be able to breathe underwater or survive in space?",
    "Would you rather go back in time and fix a mistake or go forward and see your future?",
    "Would you rather know every language or be an expert in every sport?",
    "Would you rather have a pet dinosaur or a pet dragon?",
    "Would you rather be permanently itchy or permanently sticky?",
    "Would you rather have no elbows or no knees?",
    "Would you rather always say what you're thinking or never be able to speak again?",
    "Would you rather be loved by everyone but not know it or hated but feel loved?",
    "Would you rather have all the world's knowledge or all the world's wisdom?",
    "Would you rather have a pause button for your life or a rewind button?",
    "Would you rather be the worst player on a championship team or the best on a losing team?",
    "Would you rather never be cold again or never feel hot again?",
    "Would you rather have 10 years added to your life or not age until you're 100?",
    "Would you rather be able to talk to animals or hear plants' thoughts?",
    "Would you rather have a photographic memory or only need 1 hour of sleep?",
    "Would you rather always be surrounded by noise or always be in complete silence?",
    "Would you rather have a job you hate that pays millions or a job you love at average pay?",
    "Would you rather live during the Renaissance or the distant future?",
    "Would you rather be permanently confident or permanently calm?",
    "Would you rather win a Nobel Prize or an Oscar?",
    "Would you rather have too many friends or no friends at all?",
    "Would you rather have super strength or super intelligence?",
    "Would you rather be an astronaut or a deep-sea explorer?",
    "Would you rather never experience heartbreak or never fall in love?",
    "Would you rather have every day be perfect or have a wildly unpredictable life?",
    "Would you rather be the most popular person you know or the most respected?",
    "Would you rather eat your favorite food every meal forever or never eat it again?",
    "Would you rather be 4 feet tall or 8 feet tall?",
    "Would you rather live in a house with no walls or a house with no roof?",
    "Would you rather end world hunger or end world conflict?",
    "Would you rather have hiccups forever or always have something stuck in your teeth?",
    "Would you rather have no sense of smell or no sense of taste?",
    "Would you rather everything be sticky or everything be slippery?",
    "Would you rather live forever as a 20-year-old or die at 100 having lived a full life?",
    "Would you rather have telepathy or telekinesis?",
    "Would you rather be able to control fire or water?",
    "Would you rather be a famous musician or a famous author?",
    "Would you rather be able to never get tired or never get sick?",
    "Would you rather have perfect memory or forget your worst memories?",
    "Would you rather always be overdressed or always be underdressed?",
    "Would you rather be incredible at art or incredible at science?",
    "Would you rather give up social media or give up streaming services?",
    "Would you rather speak in rhymes constantly or sing everything you say?",
    "Would you rather have a personal assistant or a personal trainer?",
    "Would you rather know every answer to every question or the question to every answer?",
    "Would you rather be the first person to discover life on another planet or the last human?",
    "Would you rather always be honest even when it hurts or always tell people what they want?",
    "Would you rather only be able to run everywhere or only be able to walk very slowly?",
    "Would you rather wake up as a different person every day or be yourself but forget everything overnight?",
    "Would you rather have no responsibilities but be bored forever or tons of responsibilities but be fulfilled?",
    "Would you rather have the ability to regenerate or the ability to become immune to anything?",
    "Would you rather be able to breathe in space or underwater?",
    "Would you rather know your future or be able to change your past?",
    "Would you rather have a dog the size of a cat or a cat the size of a dog?",
    "Would you rather be an expert at everything but a master of nothing or a master of one thing?",
    "Would you rather live in a world where everyone can read minds or where no one can lie?",
    "Would you rather be extremely good looking but stupid or very smart but unattractive?",
    "Would you rather have $1,000,000 right now or $1,000 a day for the rest of your life?",
    "Would you rather give up coffee or give up alcohol forever?",
    "Would you rather be able to communicate with aliens or go back in time to meet a historical figure?",
]

NEVER_HAVE_I_EVER = [
    "Never have I ever eaten an entire pizza by myself.",
    "Never have I ever sent a text to the wrong person and panicked.",
    "Never have I ever stayed up for more than 24 hours straight.",
    "Never have I ever laughed so hard I cried.",
    "Never have I ever pretended to be sick to skip work or school.",
    "Never have I ever eaten food I dropped on the floor.",
    "Never have I ever binge-watched an entire show in one day.",
    "Never have I ever talked to myself in the mirror for more than 5 minutes.",
    "Never have I ever forgotten someone's name immediately after they told me.",
    "Never have I ever fallen asleep in a public place.",
    "Never have I ever accidentally liked a super old Instagram post while stalking someone.",
    "Never have I ever pretended to agree when I had no idea what was being said.",
    "Never have I ever stayed in the same clothes for more than 2 days.",
    "Never have I ever re-read a conversation 20 times to analyze it.",
    "Never have I ever pretended to be busy to avoid a phone call.",
    "Never have I ever laughed at a funeral.",
    "Never have I ever eaten cereal without milk.",
    "Never have I ever googled myself.",
    "Never have I ever cried at a movie I didn't expect to.",
    "Never have I ever been convinced there was something under the bed.",
    "Never have I ever rehearsed a conversation in my head that never happened.",
    "Never have I ever pretended to understand a joke everyone was laughing at.",
    "Never have I ever had a dream that felt more real than reality.",
    "Never have I ever accidentally called a teacher 'mom' or 'dad'.",
    "Never have I ever deleted a post because it didn't get enough likes.",
    "Never have I ever snorted while laughing.",
    "Never have I ever slept with a stuffed animal past age 15.",
    "Never have I ever Googled the answer during a quiz and said I 'remembered' it.",
    "Never have I ever taken a selfie and kept retaking it 30+ times.",
    "Never have I ever gotten overly invested in a reality TV show.",
    "Never have I ever cried happy tears over something on the internet.",
    "Never have I ever been on a diet and immediately broken it the same day.",
    "Never have I ever sent a voice note instead of typing because I was too lazy.",
    "Never have I ever rage-quit a video game.",
    "Never have I ever had a genuinely weird conversation with an AI chatbot.",
    "Never have I ever made a to-do list and then done none of it.",
    "Never have I ever read the first chapter of a book and then never returned to it.",
    "Never have I ever set 10 alarms and still been late.",
    "Never have I ever started telling a story and forgotten the point halfway through.",
    "Never have I ever been completely sure I turned the stove off but gone back to check.",
    "Never have I ever started a diet on Monday and given up by Wednesday.",
    "Never have I ever spent an hour on a 5-minute task because of distractions.",
    "Never have I ever laughed at my own joke way more than anyone else did.",
    "Never have I ever bought something online at 2am that seemed like a great idea.",
    "Never have I ever checked social media within 5 minutes of waking up.",
    "Never have I ever zoned out in the middle of someone talking and nodded anyway.",
    "Never have I ever googled symptoms and convinced myself I had something terrible.",
    "Never have I ever eaten a meal while lying in bed.",
    "Never have I ever stood in front of an open fridge for 5 minutes knowing what was there.",
    "Never have I ever read the comment section when I knew I'd regret it.",
    "Never have I ever said 'I'll sleep early tonight' and then been up past 2am.",
    "Never have I ever talked to a pet as if it understood everything.",
    "Never have I ever added something I already did to my to-do list just to cross it off.",
    "Never have I ever spent more time choosing a movie than watching one.",
    "Never have I ever used 'I'll do it tomorrow' as a life strategy.",
    "Never have I ever let the laundry sit in the washer so long I had to rewash it.",
    "Never have I ever convinced myself I could remember something without writing it down — and been wrong.",
    "Never have I ever apologized for something I wasn't actually sorry for.",
    "Never have I ever pretended not to be home when someone knocked.",
    "Never have I ever had a 10-minute argument with autocorrect.",
]

HOROSCOPE = {
    "aries": [
        "🐏 <b>Aries today:</b> The stars point toward bold action. Take the risk you've been hesitating about — the cosmos has your back. Your energy is fire today; channel it wisely.",
        "🐏 <b>Aries today:</b> A surprise opportunity knocks. Don't second-guess yourself — Aries warriors charge, they don't wait. Someone close will admire your decisiveness.",
        "🐏 <b>Aries today:</b> Your competitive side fires on all cylinders. Use it to motivate, not alienate. Great things happen when your passion meets patience — even briefly.",
        "🐏 <b>Aries today:</b> Mars stirs your ambitions. A project that felt stuck suddenly has momentum. Trust your gut — it's been right more often than you admit.",
        "🐏 <b>Aries today:</b> You're radiating confidence today. Others will follow your lead whether you planned it or not. Own it. A lucky break arrives before sunset.",
    ],
    "taurus": [
        "🐂 <b>Taurus today:</b> The universe asks you to slow down and appreciate what you already have. Abundance is closer than you think — you just need to look around.",
        "🐂 <b>Taurus today:</b> Venus smiles on you today. Financial matters take a positive turn. Stay grounded and let practical wisdom guide your decisions.",
        "🐂 <b>Taurus today:</b> Your patience, which others misread as stubbornness, is your greatest asset right now. Stay the course — the reward is almost visible.",
        "🐂 <b>Taurus today:</b> Something beautiful enters your life today, possibly when you're not looking for it. Your appreciation for the finer things is valid and valuable.",
        "🐂 <b>Taurus today:</b> The earth is steady beneath you for good reason — you've built it that way. Today, enjoy the fruits of your past efforts. Comfort is earned.",
    ],
    "gemini": [
        "👯 <b>Gemini today:</b> Your mind is working overtime — multiple ideas compete for attention. Pick the one that excites you most and run with it before Mercury changes course.",
        "👯 <b>Gemini today:</b> Communication is your superpower today. Say what you've been holding back — the stars support honest conversation. A misunderstanding clears up.",
        "👯 <b>Gemini today:</b> Your adaptability is your magic. Where others see chaos, you see opportunity. Network, talk, connect — your words carry unusual weight today.",
        "👯 <b>Gemini today:</b> The twin nature in you resolves today — one clear direction emerges from the fog. Trust the decision that came to you this morning.",
        "👯 <b>Gemini today:</b> Curiosity leads to gold today. Follow the question, not the answer. A conversation leads somewhere unexpected and surprisingly wonderful.",
    ],
    "cancer": [
        "🦀 <b>Cancer today:</b> Your intuition is dialed in. That feeling you can't shake? Listen to it. The Moon supports your emotional intelligence — it's your compass.",
        "🦀 <b>Cancer today:</b> Home and family hold the key to what you need most. Step away from the noise and return to what truly nourishes your soul today.",
        "🦀 <b>Cancer today:</b> You're more resilient than you know. What's been weighing on you begins to lift. Lean into your support network — they want to help.",
        "🦀 <b>Cancer today:</b> Your empathy attracts someone who truly needs it. The kindness you give today comes back multiplied. Lead with your heart.",
        "🦀 <b>Cancer today:</b> Creative energy surges from emotional depth. Don't suppress it — let it out through art, conversation, or writing. You might surprise yourself.",
    ],
    "leo": [
        "🦁 <b>Leo today:</b> The spotlight finds you whether you're ready or not. Step into it with the confidence only a Leo can deliver. Your moment is here.",
        "🦁 <b>Leo today:</b> Generosity today creates loyalty for life. Your warmth is your crown — wear it. Someone you barely noticed is watching and admiring you.",
        "🦁 <b>Leo today:</b> The Sun energizes your natural charisma. What you start today has the power to grow into something legendary. Think big — then think bigger.",
        "🦁 <b>Leo today:</b> Creative projects align with opportunity. Your self-expression is magnetic right now. Share your work — the world is ready to receive it.",
        "🦁 <b>Leo today:</b> Leadership calls. Not everyone wants the job, but you were made for it. Guide with heart and humor. People follow warmth more than authority.",
    ],
    "virgo": [
        "♍ <b>Virgo today:</b> Your eye for detail catches what everyone else misses. That small thing you noticed? Follow it — it leads to something significant.",
        "♍ <b>Virgo today:</b> Systems and order bring clarity. Organize one corner of your life and watch how it creates space for new energy to flow in.",
        "♍ <b>Virgo today:</b> Mercury supports your analytical mind. A problem that's been frustrating you reveals a surprisingly simple solution. You've been overcomplicating it.",
        "♍ <b>Virgo today:</b> Service to others fills something in you today. Offer help without waiting to be asked — the return on this investment is immeasurable.",
        "♍ <b>Virgo today:</b> Perfectionism serves you today, but only up to a point. Know when 'done' is better than 'perfect'. Progress beats preparation eventually.",
    ],
    "libra": [
        "⚖️ <b>Libra today:</b> Balance returns to a situation that's been tilted. Venus guides your relationships today — extend an olive branch and watch the magic.",
        "⚖️ <b>Libra today:</b> Your gift for seeing both sides is needed urgently. Step in as the mediator and you'll earn deep respect. Peace is your power.",
        "⚖️ <b>Libra today:</b> Beauty and harmony surround you. Take a moment to notice — a sunset, a kind word, a song that fits perfectly. These are the universe's gifts.",
        "⚖️ <b>Libra today:</b> A decision you've been postponing becomes clear. The scales tip in one direction. Trust the clarity and commit fully.",
        "⚖️ <b>Libra today:</b> Relationships deepen today. Share something real with someone important. Authenticity attracts the kind of connection you've been craving.",
    ],
    "scorpio": [
        "🦂 <b>Scorpio today:</b> Your intensity is your gift, not your curse. Something hidden reveals itself to you alone — your perception cuts through every veil.",
        "🦂 <b>Scorpio today:</b> Transformation is your default setting. What's ending isn't a loss — it's a chrysalis. Trust the process. What emerges will be extraordinary.",
        "🦂 <b>Scorpio today:</b> Pluto sends a current of deep knowing. Trust the instinct that has no logical explanation. Your sixth sense doesn't lie.",
        "🦂 <b>Scorpio today:</b> Emotional depth creates powerful connections today. Let someone in past the surface — the intimacy is safe, and it's what you need.",
        "🦂 <b>Scorpio today:</b> Power dynamics shift in your favor. Stay calm, stay strategic. The chess pieces are moving — you already know the next three moves.",
    ],
    "sagittarius": [
        "🏹 <b>Sagittarius today:</b> Adventure calls from an unexpected direction. Say yes before you talk yourself out of it. Jupiter expands everything it touches today.",
        "🏹 <b>Sagittarius today:</b> Your optimism is infectious. Someone around you desperately needs your energy — share it freely.",
        "🏹 <b>Sagittarius today:</b> Knowledge is your currency today. Teach what you know. A random fact you share opens a door for someone else.",
        "🏹 <b>Sagittarius today:</b> Freedom and exploration are calling. Even a small adventure recharges your soul. Follow the arrow — it always points true.",
        "🏹 <b>Sagittarius today:</b> Philosophy over practicality today. Ask the big question. Don't be satisfied with a small answer. You're built for cosmic scale thinking.",
    ],
    "capricorn": [
        "🐐 <b>Capricorn today:</b> Discipline rewards you in an unexpected way. What you've been quietly building is about to get noticed. Stay humble — but stand tall.",
        "🐐 <b>Capricorn today:</b> Saturn grounds your long-term plans. Slow progress is still progress. The mountain climber doesn't doubt the summit — they just keep climbing.",
        "🐐 <b>Capricorn today:</b> Your work ethic opens a door today. Someone in authority recognizes what you bring. Don't deflect the compliment — own it.",
        "🐐 <b>Capricorn today:</b> Structure serves you, but today, leave space for the unexpected. What feels like a detour is actually a shortcut the stars planned.",
        "🐐 <b>Capricorn today:</b> Legacy thinking pays off. Your long view on something finally makes sense to someone else. Teach them what you see. It matters.",
    ],
    "aquarius": [
        "🏺 <b>Aquarius today:</b> Innovation arrives in a flash of inspiration. Write it down immediately — your brain works so fast even you can't always keep up.",
        "🏺 <b>Aquarius today:</b> Uranus sparks your rebellious genius. The rule everyone else follows? Break it thoughtfully. Your unconventional path is correct.",
        "🏺 <b>Aquarius today:</b> Your vision of the future is clearer than most. Share it selectively — not everyone can keep up, and that's okay. Lead anyway.",
        "🏺 <b>Aquarius today:</b> Community and connection align. Your network wants to hear from you. Reach out — a collaboration forms that's greater than the sum of its parts.",
        "🏺 <b>Aquarius today:</b> Humanity is your cause, not just a concept. An opportunity to make a difference presents itself quietly today. Don't overlook it.",
    ],
    "pisces": [
        "🐟 <b>Pisces today:</b> Your dreams are messages today — pay attention to the images and feelings that stay with you. Neptune speaks in metaphors.",
        "🐟 <b>Pisces today:</b> Creative waters run deep and clear. What you create today comes from a place of pure intuition. Don't overthink it — let it flow.",
        "🐟 <b>Pisces today:</b> Compassion is your superpower. A small act of kindness today creates ripples you won't see for years — but they travel far.",
        "🐟 <b>Pisces today:</b> The spiritual and the practical meet in an interesting way. Ground your dreams in one concrete step today. Magic needs earth to land on.",
        "🐟 <b>Pisces today:</b> Your empathy absorbs others' energy — remember to cleanse your emotional field today. Boundaries protect the sensitive soul you are.",
    ],
}

HANGMAN_WORDS = [
    "python", "telegram", "interface", "keyboard", "adventure", "brilliant",
    "chocolate", "dangerous", "elephant", "fibonacci", "generator", "hurricane",
    "illusion", "javascript", "knowledge", "language", "mountain", "network",
    "ordinary", "platform", "question", "reaction", "software", "technique",
    "universe", "variable", "whatever", "xylophone", "yesterday", "zoological",
    "algorithm", "backspace", "calendar", "database", "encrypted", "function",
    "gradient", "hardware", "internet", "localhost", "megabyte", "notebook",
    "overflow", "password", "quicksort", "register", "skeleton", "terminal",
    "umbrella", "vacation", "wireless", "yourself", "zeppelin", "absolute",
    "boundary", "circular", "domestic", "exterior", "freshman", "graphics",
    "hospital", "industry", "jealousy", "landmark", "magnetic", "numerous",
    "obstacle", "paradise", "quantity", "reservoir", "surprise", "thousand",
    "ultimate", "velocity", "welcome", "aquarium", "backbone", "campaign",
    "diplomat", "eighteen", "firewall", "governor", "harmless", "innocent",
    "junction", "kilogram", "listener", "momentum", "negative", "optimize",
    "peaceful", "reliable", "sandwich", "timeline", "verbalize", "workshop",
    "abstract", "beverage", "capacity", "delicate", "enormous", "favorite",
    "glamorous", "heritage", "immortal", "judicial", "kindness", "leverage",
    "mainland", "nobility", "organize", "precious", "republic", "strength",
    "together", "valuable", "wanderer", "yearbook", "airplane", "ballpark",
    "carnival", "electric", "football", "guidance", "homepage", "increase",
    "jellyfish", "longitude", "marathon", "optimize", "pancakes", "quarters",
    "roadblock", "sentence", "talented", "volcanic", "windmill", "zucchini",
    "analysis", "bathroom", "corridor", "downtown", "flagship", "grapevine",
    "headline", "imperial", "jukebox", "labyrinth", "medicine", "narrative",
    "offering", "parallel", "quotient", "rational", "spectrum", "tendency",
    "building", "darkness", "estimate", "forecast", "globally", "handsome",
    "impolite", "knowing", "lifetime", "manifest", "notorious", "populace",
    "relative", "snowfall", "textbook", "vertical", "website", "yearning",
    "alphabet", "bursting", "compete", "delivery", "eggplant", "fireplace",
    "glorious", "humorous", "incident", "judgment", "learning", "material",
    "neighbor", "obtained", "physical", "recorded", "studying", "withdraw",
    "assemble", "bargain", "careless", "describe", "fearless", "gorgeous",
    "happened", "inspiring", "mischief", "obscured", "porcelain", "quandary",
    "ridicule", "struggle", "throwing", "unifying", "withhold", "blizzard",
    "chemical", "district", "enrolled", "factual", "gigantic", "historic",
    "impacted", "jackpot", "literary", "midnight", "northern", "outright",
    "suitcase", "acoustic", "baseball", "fearsome", "grateful", "invented",
    "karaoke", "monarchy", "nightfall", "peaceful", "relaxing", "together",
    "abundant", "bravery", "criminal", "dynamic", "everyday", "festival",
    "homeless", "nonstop", "outlines", "possible", "underway", "zealously",
    "aardvark", "badminton", "catalytic", "dinosaurs", "eggshell", "falconry",
    "gallstone", "halftime", "jigsawing", "kerosene", "labrador", "meatball",
    "nickname", "offshore", "parasites", "quarrels", "randomly", "sailboat",
    "umbrella", "vandalism", "anaconda", "boulevard", "cathedral", "dragonfruit",
    "ferocious", "gradually", "hailstorm", "jailbreak", "kindling", "limitless",
    "mountains", "nightmare", "obviously", "parachute", "quicksand", "racetrack",
    "shapeless", "undertow", "wildberry", "youngster", "avocado", "blueberry",
    "donation", "earnings", "graceful", "ignition", "jalapeno", "kickstart",
    "latitude", "markdown", "nonprofit", "patience", "readable", "sunlight",
    "antibiotic", "catastrophe", "enigmatic", "fluorescent", "grasshopper",
    "hypothesis", "inspiration", "juxtapose", "kaleidoscope", "legitimacy",
    "microscope", "navigation", "obliterate", "persistence", "quarantine",
    "renaissance", "silhouette", "thunderstorm", "visualization", "watercolor",
    "archipelago", "bureaucracy", "circumference", "deliberately", "fundamental",
    "gravitational", "hydroelectric", "independence", "jurisdiction", "kindergarten",
    "labyrinthine", "metamorphosis", "nevertheless", "oceanography", "perpendicular",
    "questionnaire", "relativity", "scholarship", "triangulate", "unfortunate",
    "vaporization", "weatherproof", "xenophobia", "anthropology", "biographical",
    "compulsively", "disorganized", "evolutionary", "foreshadowing", "geographical",
    "hallucination", "idiosyncratic", "juxtaposition", "knowledgeable", "longitudinal",
    "mathematical", "northwestern", "overwhelmingly", "philosophical", "qualitatively",
    "recreational", "systematically", "transparency", "unambiguous", "voluntarily",
    "extraordinary", "generation", "helicopter", "illuminate", "liberation",
    "management", "naturalistic", "operational", "perspective", "quantitative",
    "recognition", "sovereignty", "transformation", "unprecedented", "verification",
    "worldwide", "ambidextrous", "bibliography", "catastrophic", "distinguishable",
    "elaborately", "fundamentally", "generalization", "humanitarian", "international",
    "justification", "kaleidoscopic", "multifaceted", "nonconformist", "outstandingly",
    "paradoxically", "questionable", "reprehensible", "stratospheric", "universally",
    "volunteerism", "xylography", "yesteryear", "zoogeography", "comfortable",
    "development", "engineering", "fascinating", "groundbreaking", "influential",
    "journalistic", "knowledgeable", "liberalization", "magnificent", "outstanding",
    "philosophical", "qualification", "relationship", "significantly", "traditional",
    "understanding", "victorious", "wilderness", "youngberry", "zoological",
    "catastrophe", "developmental", "establishment", "fundamental", "generational",
    "historically", "individualistic", "jurisdictional", "kaleidoscopic", "liberation",
    "mathematical", "nationalistic", "organizational", "philosophical", "qualification",
    "relationships", "systematically", "transformation", "unprecedented", "versatility",
]

FAMOUS_QUOTES = [
    ("Be the change you wish to see in the world.", "Mahatma Gandhi"),
    ("The only way to do great work is to love what you do.", "Steve Jobs"),
    ("In the middle of every difficulty lies opportunity.", "Albert Einstein"),
    ("It does not matter how slowly you go as long as you do not stop.", "Confucius"),
    ("You miss 100% of the shots you don't take.", "Wayne Gretzky"),
    ("Whether you think you can or you think you can't, you're right.", "Henry Ford"),
    ("Life is what happens when you're busy making other plans.", "John Lennon"),
    ("The future belongs to those who believe in the beauty of their dreams.", "Eleanor Roosevelt"),
    ("It is during our darkest moments that we must focus to see the light.", "Aristotle"),
    ("Tell me and I forget. Teach me and I remember. Involve me and I learn.", "Benjamin Franklin"),
    ("Success is not final, failure is not fatal: it is the courage to continue that counts.", "Winston Churchill"),
    ("Believe you can and you're halfway there.", "Theodore Roosevelt"),
    ("The only impossible journey is the one you never begin.", "Tony Robbins"),
    ("In the end, it's not the years in your life that count. It's the life in your years.", "Abraham Lincoln"),
    ("Never let the fear of striking out keep you from playing the game.", "Babe Ruth"),
    ("Life is either a daring adventure or nothing at all.", "Helen Keller"),
    ("Many of life's failures are people who did not realize how close they were to success when they gave up.", "Thomas A. Edison"),
    ("You have brains in your head. You have feet in your shoes. You can steer yourself any direction you choose.", "Dr. Seuss"),
    ("If you look at what you have in life, you'll always have more.", "Oprah Winfrey"),
    ("If you set your goals ridiculously high and it's a failure, you will fail above everyone else's success.", "James Cameron"),
    ("You don't have to be great to start, but you have to start to be great.", "Zig Ziglar"),
    ("Life is not measured by the number of breaths we take, but by the moments that take our breath away.", "Maya Angelou"),
    ("If you want to lift yourself up, lift up someone else.", "Booker T. Washington"),
    ("The secret of getting ahead is getting started.", "Mark Twain"),
    ("It always seems impossible until it's done.", "Nelson Mandela"),
    ("Don't watch the clock; do what it does. Keep going.", "Sam Levenson"),
    ("Keep your eyes on the stars, and your feet on the ground.", "Theodore Roosevelt"),
    ("When everything seems to be going against you, remember that the airplane takes off against the wind.", "Henry Ford"),
    ("It's not whether you get knocked down, it's whether you get up.", "Vince Lombardi"),
    ("Somewhere, something incredible is waiting to be known.", "Carl Sagan"),
    ("We may encounter many defeats but we must not be defeated.", "Maya Angelou"),
    ("Act as if what you do makes a difference. It does.", "William James"),
    ("Success usually comes to those who are too busy to be looking for it.", "Henry David Thoreau"),
    ("I find that the harder I work, the more luck I seem to have.", "Thomas Jefferson"),
    ("The only place where success comes before work is in the dictionary.", "Vidal Sassoon"),
    ("The way to get started is to quit talking and begin doing.", "Walt Disney"),
    ("Don't be afraid to give up the good to go for the great.", "John D. Rockefeller"),
    ("We generate fears while we sit. We overcome them by action.", "Dr. Henry Link"),
    ("The successful warrior is the average man, with laser-like focus.", "Bruce Lee"),
    ("A successful man is one who can lay a firm foundation with the bricks others have thrown at him.", "David Brinkley"),
    ("All our dreams can come true, if we have the courage to pursue them.", "Walt Disney"),
    ("Too many of us are not living our dreams because we are living our fears.", "Les Brown"),
    ("I have learned over the years that when one's mind is made up, this diminishes fear.", "Rosa Parks"),
    ("Twenty years from now you will be more disappointed by the things that you didn't do.", "Mark Twain"),
    ("The most common way people give up their power is by thinking they don't have any.", "Alice Walker"),
    ("The most courageous act is still to think for yourself. Aloud.", "Coco Chanel"),
    ("I am not a product of my circumstances. I am a product of my decisions.", "Stephen Covey"),
    ("When we strive to become better than we are, everything around us becomes better too.", "Paulo Coelho"),
    ("Happiness is not something readymade. It comes from your own actions.", "Dalai Lama"),
    ("You can't use up creativity. The more you use, the more you have.", "Maya Angelou"),
    ("Do one thing every day that scares you.", "Eleanor Roosevelt"),
    ("Opportunities don't happen. You create them.", "Chris Grosser"),
    ("Try to be a rainbow in someone's cloud.", "Maya Angelou"),
    ("If you do what you always did, you will get what you always got.", "Anonymous"),
    ("Happiness is not by chance, but by choice.", "Jim Rohn"),
    ("The best revenge is massive success.", "Frank Sinatra"),
    ("Defeat is not the worst of failures. Not to have tried is the true failure.", "George Edward Woodberry"),
    ("I choose to make the rest of my life the best of my life.", "Louise Hay"),
    ("If you're going through hell, keep going.", "Winston Churchill"),
    ("Everything you've ever wanted is on the other side of fear.", "George Addair"),
    ("There is no substitute for hard work.", "Thomas Edison"),
    ("Dreaming, after all, is a form of planning.", "Gloria Steinem"),
    ("Whatever the mind of man can conceive and believe, it can achieve.", "Napoleon Hill"),
    ("Good things come to people who wait, but better things come to those who go out and get them.", "Anonymous"),
    ("If you do what you love, you'll never work a day in your life.", "Marc Anthony"),
    ("The key to success is to focus on goals, not obstacles.", "Anonymous"),
    ("You can't cross the sea merely by standing and staring at the water.", "Rabindranath Tagore"),
    ("The harder you work for something, the greater you'll feel when you achieve it.", "Anonymous"),
    ("Don't stop when you're tired. Stop when you're done.", "Anonymous"),
    ("Wake up with determination. Go to bed with satisfaction.", "Anonymous"),
    ("Do something today that your future self will thank you for.", "Anonymous"),
    ("Little things make big days.", "Anonymous"),
    ("It's going to be hard, but hard does not mean impossible.", "Anonymous"),
    ("Don't wait for opportunity. Create it.", "Anonymous"),
    ("Sometimes we're tested not to show our weaknesses, but to discover our strengths.", "Anonymous"),
    ("The key to success is to focus on goals, not obstacles.", "Anonymous"),
    ("Dream bigger. Do bigger.", "Anonymous"),
    ("You are never too old to set another goal or to dream a new dream.", "C.S. Lewis"),
    ("The secret of success is to do the common things uncommonly well.", "John D. Rockefeller"),
    ("I alone cannot change the world, but I can cast a stone across the water to create many ripples.", "Mother Teresa"),
    ("I am not afraid of storms, for I am learning how to sail my ship.", "Louisa May Alcott"),
    ("Start where you are. Use what you have. Do what you can.", "Arthur Ashe"),
    ("What lies behind us and what lies before us are tiny matters compared to what lies within us.", "Ralph Waldo Emerson"),
    ("It is never too late to be what you might have been.", "George Eliot"),
    ("In three words I can sum up everything I've learned about life: it goes on.", "Robert Frost"),
    ("Keep your face always toward the sunshine — and shadows will fall behind you.", "Walt Whitman"),
    ("Once you choose hope, anything's possible.", "Christopher Reeve"),
    ("You do not find the happy life. You make it.", "Camilla Eyring Kimball"),
    ("Happiness is a direction, not a place.", "Sydney J. Harris"),
    ("The flower that blooms in adversity is the most rare and beautiful of all.", "Mulan"),
    ("Spread love everywhere you go. Let no one ever come to you without leaving happier.", "Mother Teresa"),
    ("When you reach the end of your rope, tie a knot in it and hang on.", "Franklin D. Roosevelt"),
    ("Always remember that you are absolutely unique. Just like everyone else.", "Margaret Mead"),
    ("Don't judge each day by the harvest you reap but by the seeds that you plant.", "Robert Louis Stevenson"),
    ("The best time to plant a tree was 20 years ago. The second best time is now.", "Chinese Proverb"),
    ("An unexamined life is not worth living.", "Socrates"),
    ("Spread love everywhere you go.", "Mother Teresa"),
    ("When you have a dream, you've got to grab it and never let go.", "Carol Burnett"),
    ("No act of kindness, no matter how small, is ever wasted.", "Aesop"),
    ("How wonderful it is that nobody need wait a single moment before starting to improve the world.", "Anne Frank"),
    ("You are enough, and you have enough, and you do enough.", "Brene Brown"),
]

EMOJI_QUIZ = [
    ("🦁👑", "The Lion King"),
    ("🕷️🕸️👨", "Spider-Man"),
    ("🧊👸❄️", "Frozen"),
    ("🦇🧔🌆", "Batman"),
    ("🚀👨‍🚀🌕", "Apollo 13"),
    ("🐟🔵💙🐠", "Finding Nemo"),
    ("👓⚡🧙‍♂️", "Harry Potter"),
    ("🧸🍯🌲🐝", "Winnie the Pooh"),
    ("🏴‍☠️🦜🚢💀", "Pirates of the Caribbean"),
    ("🌹👺✨👗", "Beauty and the Beast"),
    ("👸🍎🌲🏰", "Snow White"),
    ("🦸‍♀️⚡⭐🌟", "Wonder Woman"),
    ("🚗🏁🏎️💨", "Cars"),
    ("🦖🌿🏝️🔬", "Jurassic Park"),
    ("👽🌽💡🚲", "E.T."),
    ("🌊🚢💔🧊", "Titanic"),
    ("🤖⚡🌆🔧", "Transformers"),
    ("🧞‍♂️🪔🌙✨", "Aladdin"),
    ("💊🔴🔵🐰🕳️", "The Matrix"),
    ("🏋️‍♂️🥊🌆🥇", "Rocky"),
    ("🌪️👟🌈🏠", "Wizard of Oz"),
    ("🔦👁️🏠😱", "The Shining"),
    ("🍕🐢🌆🥋", "Ninja Turtles"),
    ("🐻🌲🍯🌸", "Brother Bear"),
    ("👨‍👩‍👧‍👦🏡🌻😊", "The Incredibles"),
    ("🎸🎶💀🎵", "Coco"),
    ("🐮🤠🚂⭐", "Toy Story"),
    ("🦊🍇🤔😒", "Fox and Grapes"),
    ("🚂💨🌄🏔️", "The Polar Express"),
    ("🎃👻💀🌙", "Halloween"),
    ("🏰🌹👿🧙", "Maleficent"),
    ("🌺🏝️🌊🎵", "Moana"),
    ("🦋🌸🌺🌸", "Butterfly Effect"),
    ("🏔️❄️🧊🐧", "Happy Feet"),
    ("🔫🏜️🌵🤠", "The Good the Bad and the Ugly"),
    ("⚗️🔬🧪💉", "Breaking Bad"),
    ("🌻🖼️🌞🎨", "Sunflowers / Van Gogh"),
    ("🦁🐯🐻🌲", "Jungle Book"),
    ("🎠🎡🎢✨", "Amusement Park"),
    ("🌊🏄🌞🌴", "Surf's Up"),
    ("🐿️🌰🍂🌲", "Over the Hedge"),
    ("🎶🎭🎪🎠", "The Greatest Showman"),
    ("👘🏯🌸⚔️", "Mulan"),
    ("🧟🧠🌍💀", "The Walking Dead"),
    ("🏕️🔦👹😱", "Friday the 13th"),
    ("🌌🚀🪐⭐", "Interstellar"),
    ("👸🐸💋🐸", "The Princess and the Frog"),
    ("🦊🌲🎃🍂", "The Fox and the Hound"),
    ("🐬🌊🔱🧜", "The Little Mermaid"),
    ("🎭🎬🌟😂", "Comedy Central"),
    ("🦸‍♂️🛡️⚡🌩️", "Thor"),
]

MOVIE_QUOTES = [
    ("May the Force be with you.", "Star Wars"),
    ("I'll be back.", "The Terminator"),
    ("To infinity and beyond!", "Toy Story"),
    ("You can't handle the truth!", "A Few Good Men"),
    ("I'm the king of the world!", "Titanic"),
    ("Why so serious?", "The Dark Knight"),
    ("Life is like a box of chocolates.", "Forrest Gump"),
    ("Here's looking at you, kid.", "Casablanca"),
    ("Just keep swimming.", "Finding Nemo"),
    ("Nobody puts Baby in a corner.", "Dirty Dancing"),
    ("You is kind, you is smart, you is important.", "The Help"),
    ("With great power comes great responsibility.", "Spider-Man"),
    ("I am your father.", "The Empire Strikes Back"),
    ("You had me at hello.", "Jerry Maguire"),
    ("After all, tomorrow is another day.", "Gone with the Wind"),
    ("I see dead people.", "The Sixth Sense"),
    ("Say hello to my little friend!", "Scarface"),
    ("There's no place like home.", "Wizard of Oz"),
    ("It's alive! It's alive!", "Frankenstein"),
    ("You talking to me?", "Taxi Driver"),
    ("We're not in Kansas anymore.", "Wizard of Oz"),
    ("Hasta la vista, baby.", "Terminator 2"),
    ("E.T. phone home.", "E.T."),
    ("Run, Forrest, run!", "Forrest Gump"),
    ("Houston, we have a problem.", "Apollo 13"),
    ("They may take our lives, but they'll never take our freedom!", "Braveheart"),
    ("Just when I thought I was out, they pull me back in.", "The Godfather Part III"),
    ("Leave the gun. Take the cannoli.", "The Godfather"),
    ("I love the smell of napalm in the morning.", "Apocalypse Now"),
    ("Bond. James Bond.", "Dr. No"),
    ("It's not who I am underneath, but what I do that defines me.", "Batman Begins"),
    ("We'll always have Paris.", "Casablanca"),
    ("My precious.", "The Lord of the Rings"),
    ("To boldly go where no man has gone before.", "Star Trek"),
    ("I volunteer as tribute.", "The Hunger Games"),
    ("Oh yes, the past can hurt. But the way I see it, you can either run from it or learn from it.", "The Lion King"),
    ("Carpe diem. Seize the day, boys.", "Dead Poets Society"),
    ("You is kind, you is smart, you is important.", "The Help"),
    ("Get to the chopper!", "Predator"),
    ("It's not a tumor!", "Kindergarten Cop"),
    ("I feel the need — the need for speed!", "Top Gun"),
    ("You can't sit with us!", "Mean Girls"),
    ("On Wednesdays we wear pink.", "Mean Girls"),
    ("Why are you so obsessed with me?", "Mean Girls"),
    ("I'm not like a regular mom, I'm a cool mom.", "Mean Girls"),
    ("Stupid is as stupid does.", "Forrest Gump"),
    ("You shall not pass!", "The Lord of the Rings"),
    ("Not all those who wander are lost.", "The Lord of the Rings"),
    ("One does not simply walk into Mordor.", "The Lord of the Rings"),
    ("I'm Batman.", "Batman (1989)"),
    ("Wakanda forever!", "Black Panther"),
]

# ─────────────────────────────────────────────────────────────────────────────
#  TEXT MANIPULATION UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def uwuify(text: str) -> str:
    replacements = [
        ("r", "w"), ("l", "w"), ("R", "W"), ("L", "W"),
        ("na", "nya"), ("Na", "Nya"), ("NA", "NYA"),
        ("ove", "uv"),
    ]
    result = text
    for old, new in replacements:
        result = result.replace(old, new)
    faces = ["uwu", "owo", "UwU", ">w<", "^w^", "( ͡° ᴥ ͡°)", "(｡◕‿‿◕｡)"]
    if result and result[-1] in ".!?":
        result = result[:-1] + " " + random.choice(faces) + result[-1]
    else:
        result += " " + random.choice(faces)
    return result

def vaporwavify(text: str) -> str:
    normal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 "
    wide   = "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９　"
    table = str.maketrans(normal, wide)
    return text.translate(table)

def flip_text(text: str) -> str:
    flip_map = {
        "a": "ɐ", "b": "q", "c": "ɔ", "d": "p", "e": "ǝ",
        "f": "ɟ", "g": "ƃ", "h": "ɥ", "i": "ᴉ", "j": "ɾ",
        "k": "ʞ", "l": "l", "m": "ɯ", "n": "u", "o": "o",
        "p": "d", "q": "b", "r": "ɹ", "s": "s", "t": "ʇ",
        "u": "n", "v": "ʌ", "w": "ʍ", "x": "x", "y": "ʎ",
        "z": "z", "A": "∀", "B": "ᗺ", "C": "Ɔ", "D": "ᗡ",
        "E": "Ǝ", "F": "Ⅎ", "G": "⅁", "H": "H", "I": "I",
        "J": "ɾ", "K": "ʞ", "L": "˥", "M": "W", "N": "N",
        "O": "O", "P": "Ԁ", "Q": "Ò", "R": "ᴚ", "S": "S",
        "T": "┴", "U": "∩", "V": "Λ", "W": "M", "X": "X",
        "Y": "⅄", "Z": "Z", "0": "0", "1": "Ɩ", "2": "ᄅ",
        "3": "Ɛ", "4": "ㄣ", "5": "ϛ", "6": "9", "7": "Ɫ",
        "8": "8", "9": "6", "!": "¡", "?": "¿", ".": "˙",
        ",": "'", "(": ")", ")": "(", "[": "]", "]": "[",
    }
    return "".join(flip_map.get(c, c) for c in reversed(text))

def to_binary(text: str) -> str:
    return " ".join(format(ord(c), "08b") for c in text)

def from_binary(text: str) -> str:
    try:
        parts = text.strip().split()
        return "".join(chr(int(p, 2)) for p in parts if p)
    except Exception:
        return "Invalid binary input."

MORSE_CODE = {
    "A": ".-", "B": "-...", "C": "-.-.", "D": "-..", "E": ".",
    "F": "..-.", "G": "--.", "H": "....", "I": "..", "J": ".---",
    "K": "-.-", "L": ".-..", "M": "--", "N": "-.", "O": "---",
    "P": ".--.", "Q": "--.-", "R": ".-.", "S": "...", "T": "-",
    "U": "..-", "V": "...-", "W": ".--", "X": "-..-", "Y": "-.--",
    "Z": "--..", "0": "-----", "1": ".----", "2": "..---",
    "3": "...--", "4": "....-", "5": ".....", "6": "-....",
    "7": "--...", "8": "---..", "9": "----.", ".": ".-.-.-",
    ",": "--..--", "?": "..--..", "!": "-.-.--", " ": "/",
}
MORSE_REVERSE = {v: k for k, v in MORSE_CODE.items()}

def to_morse(text: str) -> str:
    return " ".join(MORSE_CODE.get(c.upper(), "?") for c in text)

def from_morse(text: str) -> str:
    try:
        words = text.strip().split(" / ")
        result = ""
        for word in words:
            for code in word.split():
                result += MORSE_REVERSE.get(code, "?")
            result += " "
        return result.strip()
    except Exception:
        return "Invalid morse input."

def cursify(text: str) -> str:
    table = str.maketrans(
        "abcdefghijklmnopqrstuvwxyz",
        "𝒶𝒷𝒸𝒹𝑒𝒻𝑔𝒽𝒾𝒿𝓀𝓁𝓂𝓃𝑜𝓅𝓆𝓇𝓈𝓉𝓊𝓋𝓌𝓍𝓎𝓏"
    )
    return text.translate(table)

def tinyify(text: str) -> str:
    table = str.maketrans(
        "abcdefghijklmnopqrstuvwxyz0123456789",
        "ᵃᵇᶜᵈᵉᶠᵍʰⁱʲᵏˡᵐⁿᵒᵖᵠʳˢᵗᵘᵛʷˣʸᶻ⁰¹²³⁴⁵⁶⁷⁸⁹"
    )
    return text.translate(table)

def boldify(text: str) -> str:
    table = str.maketrans(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
        "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝐚𝐛𝐜𝐝𝐞𝐟𝐠𝐡𝐢𝐣𝐤𝐥𝐦𝐧𝐨𝐩𝐪𝐫𝐬𝐭𝐮𝐯𝐰𝐱𝐲𝐳𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗"
    )
    return text.translate(table)

def italicify(text: str) -> str:
    table = str.maketrans(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
        "𝐴𝐵𝐶𝐷𝐸𝐹𝐺𝐻𝐼𝐽𝐾𝐿𝑀𝑁𝑂𝑃𝑄𝑅𝑆𝑇𝑈𝑉𝑊𝑋𝑌𝑍𝑎𝑏𝑐𝑑𝑒𝑓𝑔ℎ𝑖𝑗𝑘𝑙𝑚𝑛𝑜𝑝𝑞𝑟𝑠𝑡𝑢𝑣𝑤𝑥𝑦𝑧"
    )
    return text.translate(table)

def strikeify(text: str) -> str:
    return "".join(c + "\u0336" for c in text)

def gen_password(length: int = 16, special: bool = True) -> str:
    chars = _string.ascii_letters + _string.digits
    if special:
        chars += "!@#$%^&*()-_=+[]{}|;:,.<>?"
    return "".join(random.choices(chars, k=length))


# ─────────────────────────────────────────────────────────────────────────────
#  ACTIVE GAMES STATE  (per-chat)
# ─────────────────────────────────────────────────────────────────────────────

hangman_games: dict = {}
numguess_games: dict = {}
wordchain_games: dict = {}
scramble_games: dict = {}
counting_games: dict = {}


# ─────────────────────────────────────────────────────────────────────────────
#  HANGMAN GAME
# ─────────────────────────────────────────────────────────────────────────────

HANGMAN_STAGES = [
    "```\n  +---+\n  |   |\n      |\n      |\n      |\n      |\n=========\n```",
    "```\n  +---+\n  |   |\n  O   |\n      |\n      |\n      |\n=========\n```",
    "```\n  +---+\n  |   |\n  O   |\n  |   |\n      |\n      |\n=========\n```",
    "```\n  +---+\n  |   |\n  O   |\n /|   |\n      |\n      |\n=========\n```",
    "```\n  +---+\n  |   |\n  O   |\n /|\\  |\n      |\n      |\n=========\n```",
    "```\n  +---+\n  |   |\n  O   |\n /|\\  |\n /    |\n      |\n=========\n```",
    "```\n  +---+\n  |   |\n  O   |\n /|\\  |\n / \\  |\n      |\n=========\n```",
]

def hangman_display(game: dict) -> str:
    wrong = game["wrong"]
    stage = HANGMAN_STAGES[min(len(wrong), len(HANGMAN_STAGES) - 1)]
    word = game["word"]
    guessed = game["guessed"]
    display = " ".join(c if c in guessed or not c.isalpha() else "_" for c in word)
    wrong_str = "  ".join(wrong) if wrong else "—"
    tries_left = 6 - len(wrong)
    return (
        f"{stage}\n"
        f"📝 Word: <code>{display}</code>\n"
        f"❌ Wrong: <code>{wrong_str}</code>\n"
        f"💔 Lives left: <b>{tries_left}</b>"
    )

@stale_guard
async def hangman_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    if not args or args[0].lower() == "start":
        if chat_id in hangman_games:
            game = hangman_games[chat_id]
            return await update.message.reply_text(
                "🎮 A hangman game is already running!\n\n" + hangman_display(game),
                parse_mode=ParseMode.HTML,
            )
        word = random.choice(HANGMAN_WORDS).lower()
        hangman_games[chat_id] = {
            "word": word, "guessed": set(), "wrong": [],
            "started_by": update.effective_user.id,
        }
        display = " ".join("_" if c.isalpha() else c for c in word)
        return await update.message.reply_text(
            f"🎮 <b>Hangman started!</b>\nGuess letters using /guess &lt;letter&gt;\n\n"
            f"{HANGMAN_STAGES[0]}\n"
            f"📝 Word: <code>{display}</code>\n"
            f"💔 Lives: <b>6</b>",
            parse_mode=ParseMode.HTML,
        )

    if args[0].lower() == "stop":
        if chat_id in hangman_games:
            word = hangman_games.pop(chat_id)["word"]
            return await update.message.reply_text(
                f"🛑 Hangman stopped. The word was: <code>{word}</code>", parse_mode=ParseMode.HTML
            )
        return await update.message.reply_text("❌ No hangman game is running.")

    if args[0].lower() == "hint":
        if chat_id not in hangman_games:
            return await update.message.reply_text("❌ No hangman game is running. Use /hangman start")
        game = hangman_games[chat_id]
        remaining = [c for c in game["word"] if c.isalpha() and c not in game["guessed"]]
        if not remaining:
            return await update.message.reply_text("All letters revealed already!")
        hint_letter = random.choice(remaining)
        game["guessed"].add(hint_letter)
        return await update.message.reply_text(
            f"💡 Hint: the letter <b>{hint_letter.upper()}</b> is in the word!\n\n" + hangman_display(game),
            parse_mode=ParseMode.HTML,
        )

    await update.message.reply_text(
        "Usage:\n/hangman start — begin\n/hangman stop — end\n/hangman hint — reveal a letter\n/guess &lt;letter&gt; — guess",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def guess_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in hangman_games:
        return await update.message.reply_text("❌ No hangman game running. Start one with /hangman")
    if not context.args:
        return await update.message.reply_text("Usage: /guess &lt;letter or full word&gt;", parse_mode=ParseMode.HTML)
    game = hangman_games[chat_id]
    guess = context.args[0].lower()
    word = game["word"]

    if len(guess) > 1:
        if guess == word:
            del hangman_games[chat_id]
            return await update.message.reply_text(
                f"🎉 <b>CORRECT!</b> {mention(update.effective_user)} guessed the word: <code>{word}</code>! 🏆",
                parse_mode=ParseMode.HTML,
            )
        game["wrong"].append(f"[{guess}]")
        if len(game["wrong"]) >= 6:
            del hangman_games[chat_id]
            return await update.message.reply_text(
                f"💀 WRONG! Game over. The word was: <code>{word}</code>", parse_mode=ParseMode.HTML
            )
        return await update.message.reply_text(
            f"❌ <code>{guess}</code> is wrong!\n\n" + hangman_display(game), parse_mode=ParseMode.HTML
        )

    letter = guess[0]
    if not letter.isalpha():
        return await update.message.reply_text("❌ Please guess a letter.")
    if letter in game["guessed"] or letter in [w.lower() for w in game["wrong"]]:
        return await update.message.reply_text(f"⚠️ Already guessed <b>{letter.upper()}</b>!", parse_mode=ParseMode.HTML)

    if letter in word:
        game["guessed"].add(letter)
        if all(c in game["guessed"] or not c.isalpha() for c in word):
            del hangman_games[chat_id]
            return await update.message.reply_text(
                f"🎉 <b>YOU WIN!</b> {mention(update.effective_user)} completed the word: <code>{word}</code>! 🏆",
                parse_mode=ParseMode.HTML,
            )
        return await update.message.reply_text(
            f"✅ <b>{letter.upper()}</b> is in the word!\n\n" + hangman_display(game), parse_mode=ParseMode.HTML
        )
    else:
        game["wrong"].append(letter.upper())
        if len(game["wrong"]) >= 6:
            del hangman_games[chat_id]
            return await update.message.reply_text(
                f"💀 <b>GAME OVER!</b> The word was: <code>{word}</code>. Better luck next time!",
                parse_mode=ParseMode.HTML,
            )
        return await update.message.reply_text(
            f"❌ <b>{letter.upper()}</b> is not in the word!\n\n" + hangman_display(game), parse_mode=ParseMode.HTML
        )


# ─────────────────────────────────────────────────────────────────────────────
#  WORD SCRAMBLE GAME
# ─────────────────────────────────────────────────────────────────────────────

def scramble_word(word: str) -> str:
    letters = list(word)
    for _ in range(20):
        random.shuffle(letters)
        if "".join(letters) != word:
            break
    return "".join(letters)


@stale_guard
async def scramble_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in scramble_games:
        g = scramble_games[chat_id]
        return await update.message.reply_text(
            f"🔤 A scramble is already active!\n\nUnscramble: <code>{g['scrambled']}</code>\nUse /unscramble &lt;word&gt;",
            parse_mode=ParseMode.HTML,
        )
    word = random.choice(HANGMAN_WORDS).lower()
    scrambled = scramble_word(word)
    scramble_games[chat_id] = {"original": word, "scrambled": scrambled, "hint_count": 0}
    await update.message.reply_text(
        f"🔤 <b>Word Scramble!</b>\n\nUnscramble this word:\n<code>{scrambled.upper()}</code>\n\n"
        f"Hint: {len(word)} letters | /unscramble &lt;word&gt; to answer | /scramblehint for a clue",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def unscramble_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in scramble_games:
        return await update.message.reply_text("❌ No scramble active. Use /scramble to start one.")
    if not context.args:
        return await update.message.reply_text("Usage: /unscramble &lt;word&gt;", parse_mode=ParseMode.HTML)
    game = scramble_games[chat_id]
    answer = context.args[0].lower().strip()
    if answer == game["original"]:
        del scramble_games[chat_id]
        await update.message.reply_text(
            f"🎉 <b>CORRECT!</b> {mention(update.effective_user)} unscrambled <code>{game['original']}</code>! 🏆",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"❌ <b>{html.escape(answer)}</b> is wrong! Scrambled: <code>{game['scrambled'].upper()}</code>",
            parse_mode=ParseMode.HTML,
        )


@stale_guard
async def scramblehint_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in scramble_games:
        return await update.message.reply_text("❌ No scramble active.")
    game = scramble_games[chat_id]
    n = game["hint_count"]
    word = game["original"]
    if n >= len(word) - 1:
        return await update.message.reply_text(f"🤔 No more hints! The word is: <code>{word}</code>", parse_mode=ParseMode.HTML)
    hint = word[:n + 1] + "_" * (len(word) - n - 1)
    game["hint_count"] += 1
    await update.message.reply_text(f"💡 Hint: <code>{hint.upper()}</code>", parse_mode=ParseMode.HTML)


# ─────────────────────────────────────────────────────────────────────────────
#  NUMBER GUESSING GAME
# ─────────────────────────────────────────────────────────────────────────────

@stale_guard
async def numguess_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    if args and args[0].lower() == "stop":
        if chat_id in numguess_games:
            g = numguess_games.pop(chat_id)
            return await update.message.reply_text(f"🛑 Game stopped. The number was: <b>{g['number']}</b>", parse_mode=ParseMode.HTML)
        return await update.message.reply_text("❌ No number game running.")

    if chat_id in numguess_games:
        g = numguess_games[chat_id]
        return await update.message.reply_text(
            f"🎲 Number game running! Guess between <b>{g['low']}</b> and <b>{g['high']}</b>\n"
            f"Attempts: <b>{g['attempts']}</b> | Use /ng &lt;number&gt;",
            parse_mode=ParseMode.HTML,
        )

    low, high = 1, 100
    if len(args) >= 2:
        try:
            low, high = int(args[0]), int(args[1])
            if low >= high:
                low, high = 1, 100
        except ValueError:
            pass

    number = random.randint(low, high)
    numguess_games[chat_id] = {"number": number, "attempts": 0, "low": low, "high": high}
    await update.message.reply_text(
        f"🎲 <b>Number Guessing Game!</b>\n\nI'm thinking of a number between <b>{low}</b> and <b>{high}</b>.\n"
        f"Use /ng &lt;number&gt; to guess! | /numguess stop to end.",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def ng_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in numguess_games:
        return await update.message.reply_text("❌ No number game running. Use /numguess to start!")
    if not context.args:
        return await update.message.reply_text("Usage: /ng &lt;number&gt;", parse_mode=ParseMode.HTML)
    try:
        guess = int(context.args[0])
    except ValueError:
        return await update.message.reply_text("❌ Please enter a valid number!")
    game = numguess_games[chat_id]
    game["attempts"] += 1
    target = game["number"]
    if guess < target:
        return await update.message.reply_text(f"📈 Too low! Attempts: <b>{game['attempts']}</b>", parse_mode=ParseMode.HTML)
    elif guess > target:
        return await update.message.reply_text(f"📉 Too high! Attempts: <b>{game['attempts']}</b>", parse_mode=ParseMode.HTML)
    else:
        del numguess_games[chat_id]
        rating = "🌟 Amazing!" if game["attempts"] <= 5 else "👍 Good job!" if game["attempts"] <= 10 else "😅 Got there eventually!"
        await update.message.reply_text(
            f"🎉 <b>CORRECT!</b> {mention(update.effective_user)} guessed <b>{target}</b> in <b>{game['attempts']}</b> attempts! {rating}",
            parse_mode=ParseMode.HTML,
        )


# ─────────────────────────────────────────────────────────────────────────────
#  WORD CHAIN GAME
# ─────────────────────────────────────────────────────────────────────────────

@stale_guard
async def wordchain_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    if args and args[0].lower() == "stop":
        if chat_id in wordchain_games:
            wordchain_games.pop(chat_id)
            return await update.message.reply_text("🛑 Word chain game stopped!")
        return await update.message.reply_text("❌ No word chain game is running.")

    if chat_id in wordchain_games:
        g = wordchain_games[chat_id]
        return await update.message.reply_text(
            f"⛓️ Word chain running!\nLast word: <code>{g['last_word']}</code>\n"
            f"Words used: <b>{len(g['used'])}</b> | /wc &lt;word&gt; to continue",
            parse_mode=ParseMode.HTML,
        )

    wordchain_games[chat_id] = {"last_word": None, "used": set(), "streak": 0}
    await update.message.reply_text(
        "⛓️ <b>Word Chain!</b>\n\nEach word must start with the last letter of the previous word.\n"
        "No repeats! Use /wc &lt;word&gt; to play. | /wordchain stop to end.",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def wc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in wordchain_games:
        return await update.message.reply_text("❌ No word chain running. Use /wordchain to start!")
    if not context.args:
        return await update.message.reply_text("Usage: /wc &lt;word&gt;", parse_mode=ParseMode.HTML)
    word = context.args[0].lower().strip()
    if not word.isalpha():
        return await update.message.reply_text("❌ Only alphabetic words allowed!")
    game = wordchain_games[chat_id]
    if word in game["used"]:
        return await update.message.reply_text(f"❌ <b>{word}</b> already used!", parse_mode=ParseMode.HTML)
    if game["last_word"] and word[0] != game["last_word"][-1]:
        return await update.message.reply_text(
            f"❌ <b>{word}</b> must start with <b>{game['last_word'][-1].upper()}</b>!",
            parse_mode=ParseMode.HTML,
        )
    game["used"].add(word)
    game["last_word"] = word
    game["streak"] = game.get("streak", 0) + 1
    streak = game["streak"]
    bonus = " 🔥" if streak % 10 == 0 and streak > 0 else ""
    await update.message.reply_text(
        f"✅ <b>{word}</b> accepted!{bonus}\n⛓️ Chain: <b>{streak}</b> | Next starts with: <b>{word[-1].upper()}</b>",
        parse_mode=ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  COUNTING GAME
# ─────────────────────────────────────────────────────────────────────────────

@stale_guard
async def counting_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    if args and args[0].lower() == "stop":
        if chat_id in counting_games:
            g = counting_games.pop(chat_id)
            return await update.message.reply_text(
                f"🛑 Counting stopped!\n📊 Final: <b>{g['count']}</b> | 🏆 Record: <b>{g['record']}</b>",
                parse_mode=ParseMode.HTML,
            )
        return await update.message.reply_text("❌ No counting game is running.")

    if args and args[0].lower() == "status":
        if chat_id in counting_games:
            g = counting_games[chat_id]
            return await update.message.reply_text(
                f"📊 Count: <b>{g['count']}</b> | 🏆 Record: <b>{g['record']}</b>",
                parse_mode=ParseMode.HTML,
            )
        return await update.message.reply_text("❌ No counting game is running.")

    if chat_id in counting_games:
        g = counting_games[chat_id]
        return await update.message.reply_text(
            f"🔢 Counting at <b>{g['count']}</b>! Send the next number.", parse_mode=ParseMode.HTML
        )

    counting_games[chat_id] = {"count": 0, "last_user": None, "record": 0}
    await update.message.reply_text(
        "🔢 <b>Counting Game!</b>\n\nCount together — send numbers in order!\n"
        "Rule: Can't count twice in a row. Send <b>1</b> to begin! /counting stop to end.",
        parse_mode=ParseMode.HTML,
    )


async def counting_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in counting_games:
        return
    msg = update.message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    if not text.isdigit():
        return
    game = counting_games[chat_id]
    expected = game["count"] + 1
    user_id = update.effective_user.id
    try:
        number = int(text)
    except ValueError:
        return
    if number != expected:
        await msg.reply_text(
            f"💥 RUINED at <b>{game['count']}</b>! Reset to 0.\n"
            f"(Expected <b>{expected}</b>, got <b>{number}</b>) | 📊 Record: <b>{game['record']}</b>",
            parse_mode=ParseMode.HTML,
        )
        game["count"] = 0
        game["last_user"] = None
        return
    if user_id == game["last_user"]:
        await msg.reply_text(
            f"💥 Can't count twice in a row! Reset from <b>{game['count']}</b>.", parse_mode=ParseMode.HTML
        )
        game["count"] = 0
        game["last_user"] = None
        return
    game["count"] = expected
    game["last_user"] = user_id
    if expected > game["record"]:
        game["record"] = expected
    milestones = {10, 25, 50, 100, 200, 500, 1000}
    if expected in milestones:
        await msg.reply_text(f"🎉 <b>Milestone!</b> Count reached <b>{expected}</b>! 🏆", parse_mode=ParseMode.HTML)


# ─────────────────────────────────────────────────────────────────────────────
#  FUN COMMANDS
# ─────────────────────────────────────────────────────────────────────────────

@stale_guard
async def joke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"😂 {random.choice(JOKES)}")


@stale_guard
async def fact_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🧠 <b>Random Fact:</b>\n\n{random.choice(FACTS)}", parse_mode=ParseMode.HTML)


@stale_guard
async def compliment_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    await update.message.reply_text(
        f"💝 {mention(target)}: {random.choice(COMPLIMENTS)}", parse_mode=ParseMode.HTML
    )


@stale_guard
async def roast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    await update.message.reply_text(
        f"🔥 {mention(target)}: {random.choice(ROASTS_EXTENDED)}", parse_mode=ParseMode.HTML
    )


@stale_guard
async def truth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    await update.message.reply_text(
        f"🎭 <b>Truth for</b> {mention(target)}:\n\n<i>{random.choice(TRUTHS)}</i>",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def dare_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    await update.message.reply_text(
        f"😈 <b>Dare for</b> {mention(target)}:\n\n<i>{random.choice(DARES)}</i>",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def wyr_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🤔 <b>Would You Rather?</b>\n\n{random.choice(WOULD_YOU_RATHER)}", parse_mode=ParseMode.HTML
    )


@stale_guard
async def nhie_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🙋 <b>Never Have I Ever...</b>\n\n<i>{random.choice(NEVER_HAVE_I_EVER)}</i>\n\n(👍 if you have, 👎 if you haven't!)",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def horoscope_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        signs = ", ".join(HOROSCOPE.keys())
        return await update.message.reply_text(
            f"♈ Usage: /horoscope &lt;sign&gt;\n\nSigns: <code>{signs}</code>", parse_mode=ParseMode.HTML
        )
    sign = context.args[0].lower()
    if sign not in HOROSCOPE:
        return await update.message.reply_text(
            f"❌ Unknown sign. Available: <code>{', '.join(HOROSCOPE.keys())}</code>", parse_mode=ParseMode.HTML
        )
    await update.message.reply_text(random.choice(HOROSCOPE[sign]), parse_mode=ParseMode.HTML)


@stale_guard
async def quote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text, author = random.choice(FAMOUS_QUOTES)
    await update.message.reply_text(
        f'💬 <i>\u201c{text}\u201d</i>\n\n\u2014 <b>{author}</b>',
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def emojiquiz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    emojis, answer = random.choice(EMOJI_QUIZ)
    await update.message.reply_text(
        f"🎭 <b>Emoji Quiz!</b>\n\n{emojis}\n\nWhat does this represent?\n/emojians &lt;answer&gt;",
        parse_mode=ParseMode.HTML,
    )
    context.chat_data["emoji_quiz_answer"] = answer.lower()


@stale_guard
async def emojians_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = context.chat_data.get("emoji_quiz_answer")
    if not answer:
        return await update.message.reply_text("❌ No emoji quiz active. Use /emojiquiz to start!")
    if not context.args:
        return await update.message.reply_text("Usage: /emojians &lt;answer&gt;", parse_mode=ParseMode.HTML)
    guess = " ".join(context.args).lower().strip()
    if guess in answer or answer in guess:
        del context.chat_data["emoji_quiz_answer"]
        await update.message.reply_text(
            f"🎉 <b>CORRECT!</b> {mention(update.effective_user)} got it!\n✅ Answer: <b>{answer}</b>",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(f"❌ Wrong! Keep guessing!", parse_mode=ParseMode.HTML)


@stale_guard
async def moviequiz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    quote, movie = random.choice(MOVIE_QUOTES)
    await update.message.reply_text(
        f"🎬 <b>Movie Quote Quiz!</b>\n\n<i>\u201c{quote}\u201d</i>\n\nWhat movie? /movieans &lt;title&gt;",
        parse_mode=ParseMode.HTML,
    )
    context.chat_data["movie_quiz_answer"] = movie.lower()


@stale_guard
async def movieans_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = context.chat_data.get("movie_quiz_answer")
    if not answer:
        return await update.message.reply_text("❌ No movie quiz active. Use /moviequiz!")
    if not context.args:
        return await update.message.reply_text("Usage: /movieans &lt;title&gt;", parse_mode=ParseMode.HTML)
    guess = " ".join(context.args).lower().strip()
    if guess in answer or any(w in guess for w in answer.split() if len(w) > 3):
        del context.chat_data["movie_quiz_answer"]
        await update.message.reply_text(
            f"🎉 <b>CORRECT!</b> {mention(update.effective_user)} got it!\n✅ Movie: <b>{answer.title()}</b>",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(f"❌ Wrong! Keep trying!", parse_mode=ParseMode.HTML)


@stale_guard
async def f_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    name = mention(target) if not err else "everyone"
    await update.message.reply_text(
        f"🫡 <b>F</b> in the chat for {name}\n\nPress F to pay respects. 🪦",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def bonk_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to bonk!")
    bonks = [
        "🔨 BONK! {u} has been sent to horny jail!",
        "🪓 {u} received the bonk of destiny!",
        "🏏 {u} got a critical hit bonk!",
        "🔨 MEGA BONK for {u}!",
        "🏒 {u} just got hockey-sticked into the sun!",
    ]
    await update.message.reply_text(random.choice(bonks).format(u=mention(target)), parse_mode=ParseMode.HTML)


@stale_guard
async def bite_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to bite!")
    bites = [
        "🦷 {a} bit {b} gently on the shoulder!",
        "😬 {a} took a chunk out of {b}!",
        "🐺 {a} bit {b} like an angry golden retriever!",
        "🩸 {a} vampire-bit {b}! Will they turn?",
        "😤 {a} couldn't resist and bit {b}!",
    ]
    await update.message.reply_text(
        random.choice(bites).format(a=mention(update.effective_user), b=mention(target)), parse_mode=ParseMode.HTML
    )


@stale_guard
async def punch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to punch!")
    punches = [
        "👊 {a} punched {b} right in the pixels!",
        "🥊 {a} landed a haymaker on {b}!",
        "💥 POW! {a} knocked {b} into next Tuesday!",
        "🥋 {a} delivered a roundhouse to {b}'s dignity!",
        "🤜 {a} yeeted their fist into {b}'s existence!",
    ]
    await update.message.reply_text(
        random.choice(punches).format(a=mention(update.effective_user), b=mention(target)), parse_mode=ParseMode.HTML
    )


@stale_guard
async def poke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to poke!")
    pokes = [
        "👉 {a} poked {b}. They probably noticed.",
        "☝️ {a} poked {b} aggressively with a thought.",
        "🫵 {a} jabbed {b} with the finger of destiny.",
        "👆 {a} poked {b}. They will never be the same.",
    ]
    await update.message.reply_text(
        random.choice(pokes).format(a=mention(update.effective_user), b=mention(target)), parse_mode=ParseMode.HTML
    )


@stale_guard
async def hug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to hug!")
    hugs = [
        "🤗 {a} wrapped {b} in a warm hug! 💕",
        "🫂 {a} gave {b} the biggest hug!",
        "🌸 {a} hugged {b} so hard their pixels blurred!",
        "💗 {a} surprise-hugged {b} from behind!",
        "🫶 {a} gave {b} a healing hug!",
    ]
    await update.message.reply_text(
        random.choice(hugs).format(a=mention(update.effective_user), b=mention(target)), parse_mode=ParseMode.HTML
    )


@stale_guard
async def kiss_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to kiss!")
    kisses = [
        "💋 {a} kissed {b}! Scandalous!",
        "😚 {a} gave {b} a little peck!",
        "🌹 {a} kissed {b}'s hand like a proper noble.",
        "😘 {a} blew {b} a kiss across the chat.",
    ]
    await update.message.reply_text(
        random.choice(kisses).format(a=mention(update.effective_user), b=mention(target)), parse_mode=ParseMode.HTML
    )


@stale_guard
async def pat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to pat!")
    pats = [
        "🫶 {a} gave {b} a comforting pat on the back.",
        "🤝 {a} patted {b} encouragingly.",
        "😌 {a} patted {b}'s head. There, there.",
        "🌟 {a} patted {b} like the good human they are.",
    ]
    await update.message.reply_text(
        random.choice(pats).format(a=mention(update.effective_user), b=mention(target)), parse_mode=ParseMode.HTML
    )


@stale_guard
async def marry_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text("❌ Reply to or mention someone to marry!")
    await update.message.reply_text(
        f"💍 {mention(update.effective_user)} got down on one knee and proposed to {mention(target)}!\n\n"
        f"Does {mention(target)} accept? 💒",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def simp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    level = random.randint(0, 100)
    bar = "💗" * (level // 10) + "🤍" * (10 - level // 10)
    verdict = (
        "🧊 Definitely not a simp." if level < 15 else
        "😐 Barely simping." if level < 30 else
        "😅 Low-key simp." if level < 50 else
        "😳 Mid-key simp confirmed." if level < 70 else
        "😭 High-key simp." if level < 85 else
        "💀 MAXIMUM SIMP. Please seek help."
    )
    await update.message.reply_text(
        f"💘 <b>Simp Meter:</b> {mention(target)}\n{bar}\n📊 <b>{level}%</b> simp\n{verdict}",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def sus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    level = random.randint(0, 100)
    bar = "🔴" * (level // 10) + "⬛" * (10 - level // 10)
    verdict = (
        "✅ Completely innocent. Probably." if level < 15 else
        "🤔 A little suspicious." if level < 35 else
        "👀 Sus behavior detected." if level < 55 else
        "🚨 Definitely sus." if level < 75 else
        "😡 HIGHLY SUSPICIOUS." if level < 90 else
        "🔴 EJECTED. 100% the impostor."
    )
    await update.message.reply_text(
        f"🟥 <b>Sus Meter:</b> {mention(target)}\n{bar}\n📊 <b>{level}%</b> sus\n{verdict}",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def iq_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    level = random.randint(0, 200)
    bar = "🧠" * min(level // 20, 10)
    verdict = (
        "💀 Rock. Literally a rock." if level < 20 else
        "😶 Below sea level." if level < 50 else
        "🙂 Functional human." if level < 80 else
        "👍 Average, respectable." if level < 100 else
        "🤓 Smart enough to know stuff." if level < 130 else
        "🧠 Big brain energy." if level < 160 else
        "🌌 Dangerous levels of intelligence."
    )
    await update.message.reply_text(
        f"🧠 <b>IQ Test:</b> {mention(target)}\n{bar}\nIQ: <b>{level}</b>\n{verdict}",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def vibe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    vibes = [
        "✨ Absolutely immaculate vibe.", "🌈 Chaotic but cute.", "🫠 Melting in a good way.",
        "😤 Unmatched energy today.", "🦋 Softcore main character vibe.",
        "🔥 Danger level: very high.", "🌊 Chill and wavy.", "🌸 Pure and soft.",
        "🖤 Aesthetic and mysterious.", "⚡ Electric chaos.", "🍵 Cozy and warm.",
        "🌙 Mysterious night energy.", "🤡 Certified clown, but loveable.",
        "💅 Unapologetically iconic.", "🧊 Cold but make it fashion.",
        "🦄 Rare and magical.", "🍂 Nostalgia core.", "🎭 Theatrical and dramatic.",
        "🌿 Earth child energy.", "💎 Precious and rare.",
    ]
    await update.message.reply_text(
        f"🌟 <b>Vibe Check:</b> {mention(target)}\n\n{random.choice(vibes)}", parse_mode=ParseMode.HTML
    )


@stale_guard
async def ship_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        user1 = update.effective_user
        user2 = update.message.reply_to_message.from_user
    else:
        return await update.message.reply_text("Usage: Reply to someone with /ship to ship yourself with them!")
    level = random.randint(0, 100)
    hearts = "❤️" * (level // 10) + "🖤" * (10 - level // 10)
    verdict = (
        "💔 Zero chemistry." if level < 10 else
        "😬 Awkward acquaintances." if level < 25 else
        "🙃 Maybe friends?" if level < 40 else
        "🤝 Solid friendship." if level < 55 else
        "💛 Good vibes together!" if level < 70 else
        "💖 Strong connection!" if level < 85 else
        "💘 SOULMATES! Do NOT separate."
    )
    n1, n2 = user1.first_name, user2.first_name
    ship_name = n1[:max(1, len(n1)//2)] + n2[max(0, len(n2)//2):]
    await update.message.reply_text(
        f"💕 <b>Ship-o-meter</b>\n\n"
        f"⚡ {html.escape(n1)} + {html.escape(n2)} = <b>{html.escape(ship_name)}</b>\n\n"
        f"{hearts}\n📊 <b>{level}%</b> compatible\n{verdict}",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def password_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    length = 16
    special = True
    for arg in (context.args or []):
        if arg.isdigit():
            length = max(8, min(64, int(arg)))
        elif arg.lower() in ("nospecial", "simple", "plain"):
            special = False
    pwd = gen_password(length, special)
    await update.message.reply_text(
        f"🔐 <b>Generated Password:</b>\n<code>{pwd}</code>\n\n"
        f"Length: {length} | Special chars: {'✅' if special else '❌'}\n"
        f"<i>⚠️ Delete this message after saving!</i>",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def uwu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /uwu &lt;text&gt; or reply to a message", parse_mode=ParseMode.HTML)
    await update.message.reply_text(uwuify(text[:500]))


@stale_guard
async def vaporwave_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /vaporwave &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(vaporwavify(text[:300]))


@stale_guard
async def fliptext_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /flip &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(flip_text(text[:200]))


@stale_guard
async def binary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text(
            "Usage: /binary encode &lt;text&gt; or /binary decode &lt;01...&gt;", parse_mode=ParseMode.HTML
        )
    mode = context.args[0].lower()
    rest = " ".join(context.args[1:])
    if mode == "encode":
        if not rest:
            return await update.message.reply_text("Please provide text to encode.")
        await update.message.reply_text(f"<code>{to_binary(rest[:100])}</code>", parse_mode=ParseMode.HTML)
    elif mode == "decode":
        if not rest:
            return await update.message.reply_text("Please provide binary to decode.")
        result = from_binary(rest)
        await update.message.reply_text(f"<code>{html.escape(result)}</code>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Use: /binary encode &lt;text&gt; or /binary decode &lt;01...&gt;", parse_mode=ParseMode.HTML)


@stale_guard
async def morse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text(
            "Usage: /morse encode &lt;text&gt; or /morse decode &lt;...---...&gt;", parse_mode=ParseMode.HTML
        )
    mode = context.args[0].lower()
    rest = " ".join(context.args[1:])
    if mode == "encode":
        if not rest:
            return await update.message.reply_text("Please provide text to encode.")
        await update.message.reply_text(f"<code>{to_morse(rest[:100])}</code>", parse_mode=ParseMode.HTML)
    elif mode == "decode":
        if not rest:
            return await update.message.reply_text("Please provide morse to decode.")
        result = from_morse(rest)
        await update.message.reply_text(f"Decoded: <code>{html.escape(result)}</code>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Use: /morse encode &lt;text&gt; or /morse decode &lt;...&gt;", parse_mode=ParseMode.HTML)


@stale_guard
async def cursive_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /cursive &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(cursify(text[:200]))


@stale_guard
async def tiny_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /tiny &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(tinyify(text[:200]))


@stale_guard
async def bold_txt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /boldtext &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(boldify(text[:200]))


@stale_guard
async def italic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /italic &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(italicify(text[:200]))


@stale_guard
async def strike_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /strike &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(strikeify(text[:200]))


@stale_guard
async def encode64_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /encode64 &lt;text&gt;", parse_mode=ParseMode.HTML)
    text = " ".join(context.args)
    encoded = base64.b64encode(text.encode()).decode()
    await update.message.reply_text(f"🔒 Base64:\n<code>{encoded}</code>", parse_mode=ParseMode.HTML)


@stale_guard
async def decode64_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /decode64 &lt;base64&gt;", parse_mode=ParseMode.HTML)
    try:
        decoded = base64.b64decode(context.args[0].encode()).decode("utf-8", errors="replace")
        await update.message.reply_text(f"🔓 Decoded:\n<code>{html.escape(decoded[:500])}</code>", parse_mode=ParseMode.HTML)
    except Exception:
        await update.message.reply_text("❌ Invalid Base64 string.")


@stale_guard
async def hash_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /hash &lt;text&gt;", parse_mode=ParseMode.HTML)
    text = " ".join(context.args).encode()
    md5 = hashlib.md5(text).hexdigest()
    sha1 = hashlib.sha1(text).hexdigest()
    sha256 = hashlib.sha256(text).hexdigest()
    await update.message.reply_text(
        f"🔐 <b>Hashes:</b>\n\nMD5: <code>{md5}</code>\nSHA1: <code>{sha1}</code>\nSHA256: <code>{sha256}</code>",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def len_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /len &lt;text&gt; or reply to a message", parse_mode=ParseMode.HTML)
    words = len(text.split())
    chars = len(text)
    lines = len(text.splitlines()) or 1
    await update.message.reply_text(
        f"📊 <b>Text Stats:</b>\n\n📝 Chars: <b>{chars}</b>\n💬 Words: <b>{words}</b>\n📋 Lines: <b>{lines}</b>",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def shout_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /shout &lt;text&gt;", parse_mode=ParseMode.HTML)
    dramatic = "  ".join(text.upper())[:400]
    await update.message.reply_text(f"📢 {dramatic}!!!")


@stale_guard
async def repeat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or len(context.args) < 2:
        return await update.message.reply_text("Usage: /repeat &lt;times&gt; &lt;text&gt;", parse_mode=ParseMode.HTML)
    try:
        times = min(int(context.args[0]), 10)
    except ValueError:
        return await update.message.reply_text("❌ First argument must be a number (max 10).")
    text = " ".join(context.args[1:])
    await update.message.reply_text("\n".join([text] * times))


@stale_guard
async def reverse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /reverse &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(text[::-1])


@stale_guard
async def mock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /mock &lt;text&gt;", parse_mode=ParseMode.HTML)
    result = "".join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(text))
    await update.message.reply_text(result)


@stale_guard
async def clap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""
    elif context.args:
        text = " ".join(context.args)
    else:
        return await update.message.reply_text("Usage: /clap &lt;text&gt;", parse_mode=ParseMode.HTML)
    await update.message.reply_text(" 👏 ".join(text.split()))


@stale_guard
async def toss_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = random.choice(["Heads 🪙", "Tails 🪙"])
    await update.message.reply_text(f"🎰 Result: <b>{result}</b>!", parse_mode=ParseMode.HTML)


@stale_guard
async def roll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sides = 6
    if context.args:
        try:
            sides = max(2, min(1000, int(context.args[0].lstrip("dD"))))
        except ValueError:
            pass
    result = random.randint(1, sides)
    await update.message.reply_text(
        f"🎲 Rolling d{sides}... <b>{result}</b>!", parse_mode=ParseMode.HTML
    )


@stale_guard
async def choose_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /choose option1, option2, option3", parse_mode=ParseMode.HTML)
    text = " ".join(context.args)
    if "," in text:
        choices = [c.strip() for c in text.split(",") if c.strip()]
    elif " or " in text.lower():
        choices = [c.strip() for c in text.lower().split(" or ") if c.strip()]
    else:
        choices = context.args
    if len(choices) < 2:
        return await update.message.reply_text("❌ Please provide at least 2 choices!")
    winner = random.choice(choices)
    await update.message.reply_text(
        f"🤔 My choice: <b>{html.escape(winner)}</b>!", parse_mode=ParseMode.HTML
    )


@stale_guard
async def rate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        if update.message.reply_to_message:
            thing = update.message.reply_to_message.text or "that"
        else:
            return await update.message.reply_text("Usage: /rate &lt;thing&gt;", parse_mode=ParseMode.HTML)
    else:
        thing = " ".join(context.args)
    score = random.randint(0, 10)
    stars = "⭐" * score + "☆" * (10 - score)
    comment = (
        "Absolutely terrible." if score < 2 else
        "Pretty bad." if score < 4 else
        "Meh, okay." if score < 6 else
        "Actually pretty good!" if score < 8 else
        "Excellent!" if score < 10 else
        "PERFECT SCORE!"
    )
    await update.message.reply_text(
        f"📊 <b>Rating:</b> {html.escape(thing[:100])}\n{stars}\n🎯 <b>{score}/10</b> — {comment}",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def weather_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /weather &lt;city&gt;", parse_mode=ParseMode.HTML)
    city = " ".join(context.args)
    url = f"https://wttr.in/{city}?format=4"
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "curl/7.68.0"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = resp.read().decode("utf-8").strip()
        if data and len(data) < 300:
            await update.message.reply_text(
                f"🌤️ <b>Weather:</b>\n<code>{html.escape(data)}</code>", parse_mode=ParseMode.HTML
            )
        else:
            raise ValueError("bad response")
    except Exception:
        await update.message.reply_text(
            f"❌ Could not fetch weather for <b>{html.escape(city)}</b>. Try a city name like 'London' or 'Tokyo'.",
            parse_mode=ParseMode.HTML,
        )


@stale_guard
async def calc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /calc &lt;expression&gt;\nExample: /calc 2 + 2 * 8", parse_mode=ParseMode.HTML)
    expr = " ".join(context.args)
    safe_chars = set("0123456789+-*/.() ")
    if any(c not in safe_chars for c in expr):
        return await update.message.reply_text("❌ Only basic math (+, -, *, /, parentheses) is supported.")
    try:
        result = eval(compile(expr, "<string>", "eval"), {"__builtins__": {}})
        await update.message.reply_text(
            f"🧮 <code>{html.escape(expr)}</code> = <b>{result}</b>", parse_mode=ParseMode.HTML
        )
    except ZeroDivisionError:
        await update.message.reply_text("❌ Division by zero!")
    except Exception:
        await update.message.reply_text("❌ Invalid expression.")


@stale_guard
async def colorhex_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        r, g, b = random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)
        hex_color = f"#{r:02X}{g:02X}{b:02X}"
    else:
        h = context.args[0].lstrip("#").upper()
        if len(h) != 6 or not all(c in "0123456789ABCDEF" for c in h):
            return await update.message.reply_text("❌ Invalid hex. Example: /color FF5733", parse_mode=ParseMode.HTML)
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        hex_color = f"#{h}"
    brightness = (r * 299 + g * 587 + b * 114) / 1000
    lumi = "Light 🌟" if brightness > 128 else "Dark 🌑"
    await update.message.reply_text(
        f"🎨 <b>Color:</b> <code>{hex_color}</code>\n🔴 R:{r} 🟢 G:{g} 🔵 B:{b}\n☀️ {lumi}",
        parse_mode=ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  MISSING ROSE FEATURES
# ─────────────────────────────────────────────────────────────────────────────

user_connections: dict = {}


@stale_guard
async def connect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat = update.effective_chat
    if chat.type == "private":
        return await update.message.reply_text("❌ Use /connect in a group chat to link it.")
    user_connections[user_id] = chat.id
    await update.message.reply_text(
        f"🔗 Connected to <b>{html.escape(chat.title or 'this group')}</b>!", parse_mode=ParseMode.HTML
    )


@stale_guard
async def disconnect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_connections:
        del user_connections[user_id]
        await update.message.reply_text("🔌 Disconnected from group.")
    else:
        await update.message.reply_text("❌ You are not connected to any group.")


@stale_guard
async def connected_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_connections:
        return await update.message.reply_text("❌ Not connected to any group.")
    chat_id = user_connections[user_id]
    try:
        chat = await context.bot.get_chat(chat_id)
        await update.message.reply_text(
            f"🔗 Connected to: <b>{html.escape(chat.title or str(chat_id))}</b>", parse_mode=ParseMode.HTML
        )
    except Exception:
        await update.message.reply_text(f"🔗 Connected to: <code>{chat_id}</code>", parse_mode=ParseMode.HTML)


@stale_guard
async def pinned_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        return await update.message.reply_text("❌ Groups only.")
    try:
        chat_info = await context.bot.get_chat(update.effective_chat.id)
        pinned = chat_info.pinned_message
        if not pinned:
            return await update.message.reply_text("📌 No pinned message in this chat.")
        await context.bot.forward_message(
            chat_id=update.effective_chat.id,
            from_chat_id=update.effective_chat.id,
            message_id=pinned.message_id,
        )
    except Exception:
        await update.message.reply_text("❌ Couldn't retrieve pinned message.")


@admin_only
async def antich_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
    off = context.args and context.args[0].lower() in ("off", "disable", "no")
    cursor.execute("UPDATE chat_settings SET anti_channel=? WHERE chat_id=?", (0 if off else 1, chat_id))
    conn.commit()
    state = "disabled" if off else "enabled"
    await update.message.reply_text(f"✅ Anti-channel messages <b>{state}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def setwarnmsg_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args and not (update.message.reply_to_message and update.message.reply_to_message.text):
        return await update.message.reply_text("Usage: /setwarnmsg &lt;message&gt;\nVars: {user}, {count}, {max}", parse_mode=ParseMode.HTML)
    chat_id = update.effective_chat.id
    text = " ".join(context.args) if context.args else update.message.reply_to_message.text
    cursor.execute("INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE chat_settings SET warn_message=? WHERE chat_id=?", (text, chat_id))
    conn.commit()
    await update.message.reply_text(f"✅ Custom warn message set!", parse_mode=ParseMode.HTML)


@admin_only
async def resetwarnmsg_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("UPDATE chat_settings SET warn_message=NULL WHERE chat_id=?", (update.effective_chat.id,))
    conn.commit()
    await update.message.reply_text("✅ Warn message reset to default.")


@stale_guard
async def privaterules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT rules FROM chat_settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return await update.message.reply_text("❌ No rules set. Use /setrules to set them.")
    user = update.effective_user
    try:
        await context.bot.send_message(
            user.id,
            f"📋 <b>Rules for {html.escape(update.effective_chat.title or 'the group')}:</b>\n\n{row[0]}",
            parse_mode=ParseMode.HTML,
        )
        await update.message.reply_text(f"📩 {mention(user)}, rules sent to your PM!", parse_mode=ParseMode.HTML)
    except Exception:
        await update.message.reply_text(f"📋 {mention(user)}, start the bot in PM first!", parse_mode=ParseMode.HTML)


@admin_only
async def cleanwelcome_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
    off = context.args and context.args[0].lower() in ("off", "disable", "no")
    cursor.execute("UPDATE chat_settings SET clean_welcome=? WHERE chat_id=?", (0 if off else 1, chat_id))
    conn.commit()
    state = "disabled" if off else "enabled"
    await update.message.reply_text(f"✅ Clean welcome <b>{state}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def antispam_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
    off = context.args and context.args[0].lower() in ("off", "disable", "no")
    cursor.execute("UPDATE chat_settings SET antispam=? WHERE chat_id=?", (0 if off else 1, chat_id))
    conn.commit()
    state = "disabled" if off else "enabled"
    await update.message.reply_text(f"🛡️ Anti-spam <b>{state}</b>.", parse_mode=ParseMode.HTML)


@admin_only
async def fban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_id FROM fed_membership WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("❌ This chat is not in any federation.")
    fed_id = row[0]
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    reason = " ".join(context.args[1:]) if context.args else "No reason given"
    cursor.execute("INSERT OR REPLACE INTO fed_bans (fed_id, user_id, reason) VALUES (?, ?, ?)",
                   (fed_id, target.id, reason))
    conn.commit()
    await update.message.reply_text(
        f"🚫 <b>Fed-Banned!</b>\n👤 {mention(target)}\n📝 Reason: <i>{html.escape(reason)}</i>",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def unfban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_id FROM fed_membership WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("❌ This chat is not in any federation.")
    fed_id = row[0]
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    cursor.execute("DELETE FROM fed_bans WHERE fed_id=? AND user_id=?", (fed_id, target.id))
    conn.commit()
    await update.message.reply_text(f"✅ {mention(target)} has been un-fedban'd.", parse_mode=ParseMode.HTML)


@stale_guard
async def fbanlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_id FROM fed_membership WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("❌ This chat is not in any federation.")
    fed_id = row[0]
    cursor.execute("SELECT user_id, reason FROM fed_bans WHERE fed_id=? LIMIT 20", (fed_id,))
    rows = cursor.fetchall()
    if not rows:
        return await update.message.reply_text("✅ No users are fedban'd.")
    lines = [f"• <code>{uid}</code> — <i>{html.escape(reason[:40])}</i>" for uid, reason in rows]
    await update.message.reply_text(f"🚫 <b>Fed Bans:</b>\n\n" + "\n".join(lines), parse_mode=ParseMode.HTML)


@admin_only
async def fednotice_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT fed_id FROM fed_membership WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text("❌ Not in a federation.")
    fed_id = row[0]
    cursor.execute("SELECT owner_id FROM federations WHERE fed_id=?", (fed_id,))
    fed_row = cursor.fetchone()
    if not fed_row or (fed_row[0] != update.effective_user.id and update.effective_user.id != OWNER_ID):
        return await update.message.reply_text("❌ Only federation owner can send notices.")
    if not context.args:
        return await update.message.reply_text("Usage: /fednotice &lt;message&gt;", parse_mode=ParseMode.HTML)
    notice = " ".join(context.args)
    cursor.execute("SELECT chat_id FROM fed_membership WHERE fed_id=?", (fed_id,))
    chats = cursor.fetchall()
    sent = 0
    for (cid,) in chats:
        try:
            await context.bot.send_message(cid, f"📢 <b>Federation Notice:</b>\n\n{html.escape(notice)}", parse_mode=ParseMode.HTML)
            sent += 1
        except Exception:
            pass
    await update.message.reply_text(f"📢 Notice sent to <b>{sent}</b> chats!", parse_mode=ParseMode.HTML)


@admin_only
async def delcmd_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
    off = context.args and context.args[0].lower() in ("off", "disable", "no")
    cursor.execute("UPDATE chat_settings SET del_commands=? WHERE chat_id=?", (0 if off else 1, chat_id))
    conn.commit()
    state = "disabled" if off else "enabled"
    await update.message.reply_text(f"🗑️ Delete command messages <b>{state}</b>.", parse_mode=ParseMode.HTML)


@stale_guard
async def globalstats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    try:
        cursor.execute("SELECT COUNT(*) FROM user_cache")
        users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT chat_id) FROM locks")
        chats = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM gban_list")
        gbans = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM warns")
        warns = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM notes")
        notes = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM filters")
        filters_count = cursor.fetchone()[0]
        await update.message.reply_text(
            f"📊 <b>Global Bot Stats</b>\n\n"
            f"👥 Users: <b>{users}</b>\n"
            f"💬 Chats: <b>{chats}</b>\n"
            f"🚫 Gbans: <b>{gbans}</b>\n"
            f"⚠️ Warns: <b>{warns}</b>\n"
            f"📌 Notes: <b>{notes}</b>\n"
            f"🔍 Filters: <b>{filters_count}</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Stats error: {e}")


@stale_guard
async def admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type == "private":
        return await update.message.reply_text("❌ Use this in a group.")
    try:
        admins_list = await chat.get_administrators()
    except Exception:
        return await update.message.reply_text("❌ Failed to fetch admin list.")
    lines = []
    for admin in admins_list:
        user = admin.user
        title = admin.custom_title or ("👑 Owner" if admin.status == "creator" else "⚙️ Admin")
        name = html.escape(user.first_name + (f" {user.last_name}" if user.last_name else ""))
        lines.append(f"{title}: <a href='tg://user?id={user.id}'>{name}</a>")
    await update.message.reply_text(
        f"👮 <b>Admins in {html.escape(chat.title or 'this chat')}:</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def invitelink_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        link = await context.bot.export_chat_invite_link(update.effective_chat.id)
        await update.message.reply_text(f"🔗 <b>Invite Link:</b>\n{link}", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def settitle_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        return await update.message.reply_text(err, parse_mode=ParseMode.HTML)
    args = context.args or []
    title = " ".join(args[1:]) if len(args) > 1 else " ".join(args)
    if not title:
        return await update.message.reply_text("Usage: /settitle [user] &lt;title&gt;", parse_mode=ParseMode.HTML)
    if len(title) > 16:
        return await update.message.reply_text("❌ Title must be 16 characters or less.")
    try:
        await context.bot.set_chat_administrator_custom_title(update.effective_chat.id, target.id, title)
        await update.message.reply_text(
            f"✅ Title for {mention(target)}: <b>{html.escape(title)}</b>", parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def announce_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if update.message.reply_to_message:
        try:
            await context.bot.pin_chat_message(chat_id, update.message.reply_to_message.message_id)
            await update.message.reply_text("📌 Message pinned as announcement!")
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to pin: {e}")
    elif context.args:
        text = " ".join(context.args)
        msg = await context.bot.send_message(
            chat_id, f"📢 <b>Announcement</b>\n\n{html.escape(text)}", parse_mode=ParseMode.HTML
        )
        try:
            await context.bot.pin_chat_message(chat_id, msg.message_id)
        except Exception:
            pass
    else:
        await update.message.reply_text("Usage: /announce &lt;text&gt; or reply to a message", parse_mode=ParseMode.HTML)


@admin_only
async def setfloodaction_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text(
            "Usage: /setfloodaction &lt;ban|kick|mute|tban|tmute&gt;", parse_mode=ParseMode.HTML
        )
    action = context.args[0].lower()
    valid = {"ban", "kick", "mute", "tban", "tmute"}
    if action not in valid:
        return await update.message.reply_text(f"❌ Invalid. Choose: {', '.join(valid)}")
    chat_id = update.effective_chat.id
    duration = context.args[1] if len(context.args) > 1 and action in ("tban", "tmute") else "1h"
    value = f"{action}:{duration}" if action in ("tban", "tmute") else action
    cursor.execute("INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
    cursor.execute("UPDATE chat_settings SET flood_action=? WHERE chat_id=?", (value, chat_id))
    conn.commit()
    await update.message.reply_text(f"✅ Flood action: <b>{value}</b>", parse_mode=ParseMode.HTML)


@stale_guard
async def floodaction_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT flood_action FROM chat_settings WHERE chat_id=?", (chat_id,))
    row = cursor.fetchone()
    action = row[0] if row and row[0] else "kick"
    await update.message.reply_text(f"⚡ Flood action: <b>{action}</b>", parse_mode=ParseMode.HTML)


@admin_only
async def stopall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("SELECT COUNT(*) FROM filters WHERE chat_id=?", (chat_id,))
    count = cursor.fetchone()[0]
    if count == 0:
        return await update.message.reply_text("❌ No filters to remove.")
    cursor.execute("DELETE FROM filters WHERE chat_id=?", (chat_id,))
    conn.commit()
    await update.message.reply_text(f"✅ Removed all <b>{count}</b> filters.", parse_mode=ParseMode.HTML)


@stale_guard
async def privatenote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not context.args:
        return await update.message.reply_text("Usage: /privatenote &lt;notename&gt;", parse_mode=ParseMode.HTML)
    name = context.args[0].lower()
    cursor.execute("SELECT content FROM notes WHERE chat_id=? AND name=?", (chat_id, name))
    row = cursor.fetchone()
    if not row:
        return await update.message.reply_text(f"❌ Note <code>{name}</code> not found.", parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_message(user_id, f"📌 <b>Note — {name}:</b>\n\n{row[0]}", parse_mode=ParseMode.HTML)
        await update.message.reply_text(f"📩 Note sent to your PM!", parse_mode=ParseMode.HTML)
    except Exception:
        await update.message.reply_text("❌ Couldn't send. Start the bot in PM first!", parse_mode=ParseMode.HTML)


@stale_guard
async def chatinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type == "private":
        return await update.message.reply_text("❌ Use in a group.")
    try:
        chat_info = await context.bot.get_chat(chat.id)
        members = await context.bot.get_chat_member_count(chat.id)
        await update.message.reply_text(
            f"💬 <b>Chat Info</b>\n\n"
            f"📛 <b>{html.escape(chat_info.title or '—')}</b>\n"
            f"🆔 <code>{chat.id}</code>\n"
            f"🔗 @{chat_info.username}" if chat_info.username else f"💬 <b>Chat Info</b>\n\n📛 <b>{html.escape(chat_info.title or '—')}</b>\n🆔 <code>{chat.id}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    try:
        members = await context.bot.get_chat_member_count(chat.id)
        chat_info = await context.bot.get_chat(chat.id)
        lines = [
            f"💬 <b>Chat Info</b>",
            f"📛 Name: <b>{html.escape(chat_info.title or '—')}</b>",
            f"🆔 ID: <code>{chat.id}</code>",
            f"📊 Type: <b>{chat.type.title()}</b>",
            f"👥 Members: <b>{members}</b>",
            f"📌 Description: <i>{html.escape((chat_info.description or '—')[:80])}</i>",
        ]
        if chat_info.username:
            lines.insert(3, f"🔗 @{chat_info.username}")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@stale_guard
async def userinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target, err = await get_target(update, context)
    if err:
        target = update.effective_user
    chat = update.effective_chat
    try:
        member = await chat.get_member(target.id)
        status = member.status.replace("_", " ").title()
    except Exception:
        status = "Unknown"
    cursor.execute("SELECT COUNT(*) FROM warns WHERE chat_id=? AND user_id=?", (chat.id, target.id))
    warns_count = cursor.fetchone()[0]
    cursor.execute("SELECT 1 FROM gban_list WHERE user_id=?", (target.id,))
    gbanned = "🚫 Yes" if cursor.fetchone() else "✅ No"
    full_name = html.escape(target.first_name + (f" {target.last_name}" if target.last_name else ""))
    await update.message.reply_text(
        f"👤 <b>User Info</b>\n\n"
        f"📛 {full_name}\n"
        f"🆔 <code>{target.id}</code>\n"
        f"🔗 {'@' + target.username if target.username else '—'}\n"
        f"🤖 Bot: {'Yes' if target.is_bot else 'No'}\n"
        f"💬 Status: <b>{status}</b>\n"
        f"⚠️ Warns: <b>{warns_count}</b>\n"
        f"🌐 Gbanned: <b>{gbanned}</b>",
        parse_mode=ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  GIVEAWAY SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

giveaway_active: dict = {}


@admin_only
async def giveaway_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        if chat_id in giveaway_active:
            g = giveaway_active[chat_id]
            return await update.message.reply_text(
                f"🎉 Active Giveaway: <b>{html.escape(g['prize'])}</b>\n"
                f"👥 Entries: <b>{len(g['entries'])}</b> | /gend to pick a winner",
                parse_mode=ParseMode.HTML,
            )
        return await update.message.reply_text("Usage: /giveaway &lt;prize&gt;", parse_mode=ParseMode.HTML)
    if chat_id in giveaway_active:
        return await update.message.reply_text("❌ A giveaway is already running! Use /gend first.")
    prize = " ".join(context.args)
    giveaway_active[chat_id] = {"prize": prize, "entries": set(), "host": update.effective_user.id}
    await update.message.reply_text(
        f"🎉 <b>GIVEAWAY!</b>\n\n🏆 Prize: <b>{html.escape(prize)}</b>\n"
        f"🎫 Use /genter to enter!\n📢 Hosted by: {mention(update.effective_user)}\n\nGood luck! 🍀",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def genter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in giveaway_active:
        return await update.message.reply_text("❌ No giveaway running.")
    user_id = update.effective_user.id
    g = giveaway_active[chat_id]
    if user_id in g["entries"]:
        return await update.message.reply_text("✅ You're already entered!")
    g["entries"].add(user_id)
    await update.message.reply_text(
        f"🎫 {mention(update.effective_user)} entered! Total: <b>{len(g['entries'])}</b>",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def gend_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in giveaway_active:
        return await update.message.reply_text("❌ No giveaway running.")
    g = giveaway_active.pop(chat_id)
    entries = list(g["entries"])
    if not entries:
        return await update.message.reply_text("😢 No one entered the giveaway!")
    winner_id = random.choice(entries)
    try:
        winner = await context.bot.get_chat_member(chat_id, winner_id)
        winner_str = mention(winner.user)
    except Exception:
        winner_str = f"<code>{winner_id}</code>"
    await update.message.reply_text(
        f"🎊 <b>Giveaway Over!</b>\n🏆 Prize: <b>{html.escape(g['prize'])}</b>\n"
        f"🎉 Winner: {winner_str}\n👥 Entries: <b>{len(entries)}</b>\n\nCongratulations! 🥳",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def quickpoll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage: /quickpoll &lt;question&gt;", parse_mode=ParseMode.HTML)
    question = " ".join(context.args)[:255]
    try:
        await context.bot.send_poll(
            chat_id=update.effective_chat.id,
            question=question,
            options=["✅ Yes", "❌ No", "🤔 Maybe"],
            is_anonymous=False,
        )
        try:
            await update.message.delete()
        except Exception:
            pass
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def vote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or len(context.args) < 3:
        return await update.message.reply_text(
            "Usage: /vote &lt;question&gt;; &lt;option1&gt;; &lt;option2&gt;", parse_mode=ParseMode.HTML
        )
    raw = " ".join(context.args)
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    if len(parts) < 3:
        return await update.message.reply_text("❌ Need question + at least 2 options separated by ;")
    question = parts[0][:255]
    options = parts[1:11]
    try:
        await context.bot.send_poll(
            chat_id=update.effective_chat.id,
            question=question,
            options=options,
            is_anonymous=True,
        )
        try:
            await update.message.delete()
        except Exception:
            pass
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


@admin_only
async def antiraid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cursor.execute("INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
    off = context.args and context.args[0].lower() in ("off", "disable")
    cursor.execute("UPDATE chat_settings SET antiraid=? WHERE chat_id=?", (0 if off else 1, chat_id))
    conn.commit()
    state = "disabled" if off else "enabled"
    await update.message.reply_text(f"🛡️ Anti-raid mode <b>{state}</b>!", parse_mode=ParseMode.HTML)


# ─────────────────────────────────────────────────────────────────────────────
#  EXTENDED HELP COMMANDS
# ─────────────────────────────────────────────────────────────────────────────

@stale_guard
async def helpgames_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎮 <b>Games Help</b>\n\n"
        "/hangman [start|stop|hint] — hangman game\n"
        "/guess &lt;letter&gt; — guess in hangman\n"
        "/scramble — word scramble\n"
        "/unscramble &lt;word&gt; — answer\n"
        "/scramblehint — scramble hint\n"
        "/numguess [low] [high] — number guessing\n"
        "/ng &lt;number&gt; — submit guess\n"
        "/wordchain [stop] — word chain\n"
        "/wc &lt;word&gt; — submit word\n"
        "/counting [stop|status] — counting game\n"
        "/emojiquiz — emoji quiz\n"
        "/emojians &lt;answer&gt; — answer\n"
        "/moviequiz — movie quote quiz\n"
        "/movieans &lt;answer&gt; — answer\n"
        "/giveaway &lt;prize&gt; — start giveaway (admin)\n"
        "/genter — enter giveaway\n"
        "/gend — end giveaway (admin)",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def helpfun_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎭 <b>Fun Commands Help</b>\n\n"
        "/joke — random joke\n"
        "/fact — random fact\n"
        "/quote — famous quote\n"
        "/compliment [user] — compliment\n"
        "/roast [user] — roast\n"
        "/truth [user] — truth question\n"
        "/dare [user] — dare challenge\n"
        "/wyr — would you rather\n"
        "/nhie — never have I ever\n"
        "/horoscope &lt;sign&gt; — daily horoscope\n"
        "/kiss [user] | /hug [user] | /bite [user]\n"
        "/bonk [user] | /punch [user] | /poke [user]\n"
        "/pat [user] | /marry [user] | /ship\n"
        "/f [user] — press F\n"
        "/simp [user] | /sus [user] | /iq [user]\n"
        "/vibe [user] | /rate &lt;thing&gt;\n"
        "/toss — flip coin | /roll [d6] — roll die\n"
        "/choose a, b, c — pick one\n"
        "/password [length] — generate password\n"
        "/weather &lt;city&gt; — weather\n"
        "/calc &lt;expr&gt; — calculator\n"
        "/color [#hex] — color info",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def helptext_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✏️ <b>Text Tools Help</b>\n\n"
        "/uwu &lt;text&gt; — UwUify\n"
        "/vaporwave &lt;text&gt; — vaporwave\n"
        "/flip &lt;text&gt; — flip upside down\n"
        "/cursive &lt;text&gt; — cursive style\n"
        "/tiny &lt;text&gt; — superscript\n"
        "/boldtext &lt;text&gt; — unicode bold\n"
        "/italic &lt;text&gt; — unicode italic\n"
        "/strike &lt;text&gt; — strikethrough\n"
        "/shout &lt;text&gt; — DRAMATIC SHOUT\n"
        "/repeat &lt;n&gt; &lt;text&gt; — repeat n times\n"
        "/reverse &lt;text&gt; — reverse text\n"
        "/mock &lt;text&gt; — SpOnGeBoB mock\n"
        "/clap &lt;text&gt; — 👏 clap it\n"
        "/binary encode|decode &lt;text&gt;\n"
        "/morse encode|decode &lt;text&gt;\n"
        "/encode64 &lt;text&gt; — base64 encode\n"
        "/decode64 &lt;b64&gt; — base64 decode\n"
        "/hash &lt;text&gt; — MD5/SHA1/SHA256\n"
        "/len &lt;text&gt; — char/word count",
        parse_mode=ParseMode.HTML,
    )


@stale_guard
async def helprose_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌹 <b>Extra Rose-like Features</b>\n\n"
        "<b>Connection:</b>\n"
        "/connect — link group\n"
        "/disconnect — unlink\n"
        "/connected — show linked group\n\n"
        "<b>Federation:</b>\n"
        "/fban [user] [reason] — fed ban\n"
        "/unfban [user] — remove fed ban\n"
        "/fbanlist — list fed bans\n"
        "/fednotice &lt;text&gt; — broadcast to all fed chats\n\n"
        "<b>Admin Extras:</b>\n"
        "/settitle [user] &lt;title&gt; — custom title\n"
        "/announce &lt;text&gt; — pin announcement\n"
        "/invitelink — get invite link\n"
        "/admins — list admins\n\n"
        "<b>Moderation:</b>\n"
        "/setfloodaction &lt;action&gt;\n"
        "/floodaction — show current\n"
        "/setwarnmsg &lt;text&gt; — custom warn\n"
        "/resetwarnmsg — reset warn msg\n"
        "/antispam [off] | /antiraid [off]\n"
        "/antich [off] | /delcmd [off]\n"
        "/stopall — clear all filters\n\n"
        "<b>Info:</b>\n"
        "/userinfo [user] | /chatinfo\n"
        "/pinned | /globalstats (owner)\n\n"
        "<b>Extras:</b>\n"
        "/cleanwelcome [off]\n"
        "/privaterules | /privatenote &lt;name&gt;\n"
        "/quickpoll &lt;question&gt;\n"
        "/vote &lt;q&gt;; &lt;opt1&gt;; &lt;opt2&gt;",
        parse_mode=ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════
#  APPLICATION SETUP
# ══════════════════════════════════════════════════════════
def main():
    init_mongo()
    _init_extra_tables()
    app = Application.builder().token(BOT_TOKEN).build()

    # Core
    app.add_handler(CommandHandler("start",    start))
    app.add_handler(CommandHandler("help",     help_cmd))

    # Moderation
    app.add_handler(CommandHandler("ban",      ban_cmd))
    app.add_handler(CommandHandler("sban",     sban_cmd))
    app.add_handler(CommandHandler("unban",    unban_cmd))
    app.add_handler(CommandHandler("kick",     kick_cmd))
    app.add_handler(CommandHandler("skick",    skick_cmd))
    app.add_handler(CommandHandler("kickme",   kickme_cmd))
    app.add_handler(CommandHandler("banme",    banme_cmd))
    app.add_handler(CommandHandler("mute",     mute_cmd))
    app.add_handler(CommandHandler("unmute",   unmute_cmd))
    app.add_handler(CommandHandler("smute",    smute_cmd))
    app.add_handler(CommandHandler("tmute",    tmute_cmd))
    app.add_handler(CommandHandler("stmute",   stmute_cmd))
    app.add_handler(CommandHandler("tban",     tban_cmd))
    app.add_handler(CommandHandler("promote",  promote_cmd))
    app.add_handler(CommandHandler("demote",   demote_cmd))
    app.add_handler(CommandHandler("title",    title_cmd))
    app.add_handler(CommandHandler("pin",      pin_cmd))
    app.add_handler(CommandHandler("unpin",    unpin_cmd))
    app.add_handler(CommandHandler("purge",    purge_cmd))
    app.add_handler(CommandHandler("spurge",   spurge_cmd))
    app.add_handler(CommandHandler("del",      del_cmd))
    app.add_handler(CommandHandler("adminlist",adminlist_cmd))
    app.add_handler(CommandHandler("report",   report_cmd))
    app.add_handler(CommandHandler("invite",   invite_cmd))

    # Global ban
    app.add_handler(CommandHandler("gban",     gban_cmd))
    app.add_handler(CommandHandler("ungban",   ungban_cmd))
    app.add_handler(CommandHandler("gbanlist", gbanlist_cmd))

    # Warn system
    app.add_handler(CommandHandler("warn",          warn_cmd))
    app.add_handler(CommandHandler("warns",         warns_cmd))
    app.add_handler(CommandHandler("unwarn",        unwarn_cmd))
    app.add_handler(CommandHandler("resetwarns",    reset_warns_cmd))
    app.add_handler(CommandHandler("setwarnlimit",  set_warn_limit_cmd))
    app.add_handler(CommandHandler("setwarnaction", set_warn_action_cmd))
    app.add_handler(CommandHandler("strongwarn",    strongwarn_cmd))

    # Notes
    app.add_handler(CommandHandler("save",     save_note_cmd))
    app.add_handler(CommandHandler("get",      get_note_cmd))
    app.add_handler(CommandHandler("clear",    clear_note_cmd))
    app.add_handler(CommandHandler("clearall", clearall_notes_cmd))
    app.add_handler(CommandHandler("notes",    list_notes_cmd))

    # Filters
    app.add_handler(CommandHandler("filter",  add_filter_cmd))
    app.add_handler(CommandHandler("stop",    stop_filter_cmd))
    app.add_handler(CommandHandler("stopall", stopall_filters_cmd))
    app.add_handler(CommandHandler("filters", list_filters_cmd))

    # Rules
    app.add_handler(CommandHandler("rules",        rules_cmd))
    app.add_handler(CommandHandler("setrules",     set_rules_cmd))
    app.add_handler(CommandHandler("clearrules",   clear_rules_cmd))
    app.add_handler(CommandHandler("privaterules", privaterules_cmd))

    # Welcome / Goodbye
    app.add_handler(CommandHandler("setwelcome",   set_welcome_cmd))
    app.add_handler(CommandHandler("setgoodbye",   set_goodbye_cmd))
    app.add_handler(CommandHandler("resetwelcome", reset_welcome_cmd))
    app.add_handler(CommandHandler("resetgoodbye", reset_goodbye_cmd))
    app.add_handler(CommandHandler("welcome",      toggle_welcome_cmd))
    app.add_handler(CommandHandler("goodbye",      toggle_goodbye_cmd))
    app.add_handler(CommandHandler("cleanwelcome", cleanwelcome_cmd))
    app.add_handler(CommandHandler("cleanservice", cleanservice_cmd))

    # Locks
    app.add_handler(CommandHandler("lock",        lock_cmd))
    app.add_handler(CommandHandler("unlock",      unlock_cmd))
    app.add_handler(CommandHandler("locks",       locks_cmd))
    app.add_handler(CommandHandler("adminlock",   adminlock_cmd))
    app.add_handler(CommandHandler("adminunlock", adminunlock_cmd))
    app.add_handler(CommandHandler("adminlocks",  adminlocks_cmd))

    # Flood
    app.add_handler(CommandHandler("setflood",       set_flood_cmd))
    app.add_handler(CommandHandler("setfloodaction", set_flood_action_cmd))
    app.add_handler(CommandHandler("flood",          flood_cmd))

    # Blacklist
    app.add_handler(CommandHandler("addblacklist",  add_blacklist_cmd))
    app.add_handler(CommandHandler("rmblacklist",   rm_blacklist_cmd))
    app.add_handler(CommandHandler("blacklist",     blacklist_cmd))
    app.add_handler(CommandHandler("blacklistmode", blacklistmode_cmd))

    # AFK
    app.add_handler(CommandHandler("afk", afk_cmd))

    # Info
    app.add_handler(CommandHandler("info",     info_cmd))
    app.add_handler(CommandHandler("whois",    whois_cmd))
    app.add_handler(CommandHandler("chatinfo", chatinfo_cmd))
    app.add_handler(CommandHandler("id",       id_cmd))
    app.add_handler(CommandHandler("stats",    stats_cmd))

    # Approval
    app.add_handler(CommandHandler("approve",      approve_cmd))
    app.add_handler(CommandHandler("unapprove",    unapprove_cmd))
    app.add_handler(CommandHandler("approved",     approved_cmd))
    app.add_handler(CommandHandler("unapproveall", unapproveall_cmd))

    # Whitelist
    app.add_handler(CommandHandler("whitelist",   whitelist_cmd))
    app.add_handler(CommandHandler("unwhitelist", unwhitelist_cmd))
    app.add_handler(CommandHandler("whitelisted", whitelisted_cmd))

    # Disable/Enable
    app.add_handler(CommandHandler("disable",  disable_cmd))
    app.add_handler(CommandHandler("enable",   enable_cmd))
    app.add_handler(CommandHandler("disabled", disabled_cmd))

    # Anti-bot
    app.add_handler(CommandHandler("antibot",  antibot_cmd))

    # Log channel
    app.add_handler(CommandHandler("setlog",   setlog_cmd))

    # Connect / Broadcast
    app.add_handler(CommandHandler("connect",    connect_cmd))
    app.add_handler(CommandHandler("disconnect", disconnect_cmd))
    app.add_handler(CommandHandler("broadcast",  broadcast_cmd))

    # Bot stats / groups / cache (owner)
    app.add_handler(CommandHandler("botstats",  botstats_cmd))
    app.add_handler(CommandHandler("botgroups", botgroups_cmd))
    app.add_handler(CommandHandler("cache",     cache_cmd))

    # Tag-all stop
    app.add_handler(CommandHandler("cancel",  cancel_tagall_cmd))
    app.add_handler(CommandHandler("stoptag", cancel_tagall_cmd))

    # Federation
    app.add_handler(CommandHandler("newfed",    newfed_cmd))
    app.add_handler(CommandHandler("joinfed",   joinfed_cmd))
    app.add_handler(CommandHandler("leavefed",  leavefed_cmd))
    app.add_handler(CommandHandler("fban",      fban_cmd))
    app.add_handler(CommandHandler("unfban",    unfban_cmd))
    app.add_handler(CommandHandler("fedinfo",   fedinfo_cmd))
    app.add_handler(CommandHandler("fedadmins", fedadmins_cmd))

    # ── Extended moderation ────────────────────────────────────────────
    app.add_handler(CommandHandler("muteall",      muteall_cmd))
    app.add_handler(CommandHandler("unmuteall",    unmuteall_cmd))
    app.add_handler(CommandHandler("warnmax",      warn_max_cmd))
    app.add_handler(CommandHandler("unsetlog",     unsetlog_cmd))
    app.add_handler(CommandHandler("unmuteuser",   unmuteuser_cmd))
    app.add_handler(CommandHandler("unbanuser",    unbanuser_cmd))
    app.add_handler(CommandHandler("delmsg",       delmsg_cmd))
    app.add_handler(CommandHandler("sql",          sql_cmd))
    app.add_handler(CommandHandler("echo",         echo_cmd))
    app.add_handler(CommandHandler("ghost",        ghost_cmd))

    # ── Captcha / verification ─────────────────────────────────────────
    app.add_handler(CommandHandler("captcha",      captcha_cmd))
    app.add_handler(CommandHandler("forcesub",     forcesub_cmd))

    # ── Anti-raid ──────────────────────────────────────────────────────
    app.add_handler(CommandHandler("antiraid",     antiraid_cmd))

    # ── Filters (caps / emoji / links / forward) ───────────────────────
    app.add_handler(CommandHandler("capsfilter",   capsfilter_cmd))
    app.add_handler(CommandHandler("emojifilter",  emojifilter_cmd))
    app.add_handler(CommandHandler("allowlink",    allowlink_cmd))
    app.add_handler(CommandHandler("rmlink",       rmlink_cmd))
    app.add_handler(CommandHandler("allowedlinks", allowedlinks_cmd))
    app.add_handler(CommandHandler("antiforward",  antiforward_cmd))
    app.add_handler(CommandHandler("lockx",        lockx_cmd))
    app.add_handler(CommandHandler("unlockx",      unlockx_cmd))

    # ── Karma ──────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("karma",        karma_cmd))
    app.add_handler(CommandHandler("ktop",         ktop_cmd))
    app.add_handler(CommandHandler("kresetall",    kresetall_cmd))

    # ── Custom commands ────────────────────────────────────────────────
    app.add_handler(CommandHandler("addcmd",       addcmd_cmd))
    app.add_handler(CommandHandler("rmcmd",        rmcmd_cmd))
    app.add_handler(CommandHandler("cmds",         cmds_cmd))

    # ── Statistics / activity ──────────────────────────────────────────
    app.add_handler(CommandHandler("topactive",    topactive_cmd))
    app.add_handler(CommandHandler("msgcount",     msgcount_cmd))
    app.add_handler(CommandHandler("resetactivity",resetactivity_cmd))
    app.add_handler(CommandHandler("mediastats",   mediastats_cmd))
    app.add_handler(CommandHandler("membercount",  membercount_cmd))
    app.add_handler(CommandHandler("ping",         ping_cmd))

    # ── Quotes ─────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("quote",        quote_cmd))
    app.add_handler(CommandHandler("delquote",     delquote_cmd))
    app.add_handler(CommandHandler("quotes",       quotes_cmd))

    # ── Reminders ──────────────────────────────────────────────────────
    app.add_handler(CommandHandler("remind",           remind_cmd))
    app.add_handler(CommandHandler("reminders",        reminders_cmd))
    app.add_handler(CommandHandler("cancelreminder",   cancelreminder_cmd))

    # ── Scheduled messages ─────────────────────────────────────────────
    app.add_handler(CommandHandler("schedule",         schedule_cmd))
    app.add_handler(CommandHandler("cancelschedule",   cancelschedule_cmd))

    # ── Auto-delete ────────────────────────────────────────────────────
    app.add_handler(CommandHandler("autodelete",   autodelete_cmd))

    # ── Slowmode ───────────────────────────────────────────────────────
    app.add_handler(CommandHandler("slowmode",     slowmode_cmd))

    # ── Backup / restore ───────────────────────────────────────────────
    app.add_handler(CommandHandler("backup",       backup_cmd))
    app.add_handler(CommandHandler("restore",      restore_cmd))

    # ── Fun commands ───────────────────────────────────────────────────
    app.add_handler(CommandHandler("coinflip",     coinflip_cmd))
    app.add_handler(CommandHandler("dice",         dice_cmd))
    app.add_handler(CommandHandler(["8ball", "eightball"], eightball_cmd))
    app.add_handler(CommandHandler("rps",          rps_cmd))
    app.add_handler(CommandHandler("roast",        roast_cmd))
    app.add_handler(CommandHandler("compliment",   compliment_cmd))
    app.add_handler(CommandHandler("hug",          hug_cmd))
    app.add_handler(CommandHandler("slap",         slap_cmd))
    app.add_handler(CommandHandler("pat",          pat_cmd))
    app.add_handler(CommandHandler("ship",         ship_cmd))
    app.add_handler(CommandHandler("love",         love_cmd))
    app.add_handler(CommandHandler("trivia",       trivia_cmd))
    app.add_handler(CommandHandler("triviascore",  triviascore_cmd))
    app.add_handler(CommandHandler("roll",         roll_cmd))
    app.add_handler(CommandHandler("choose",       choose_cmd))
    app.add_handler(CommandHandler("rate",         rate_cmd))
    app.add_handler(CommandHandler("pp",           pp_cmd))
    app.add_handler(CommandHandler("gay",          gay_cmd))
    app.add_handler(CommandHandler("iq",           iq_cmd))
    app.add_handler(CommandHandler("howcringe",    howcringe_cmd))
    app.add_handler(CommandHandler("joke",         joke_cmd))
    app.add_handler(CommandHandler("fact",         fact_cmd))
    app.add_handler(CommandHandler("meme",         meme_cmd))
    app.add_handler(CommandHandler("truth",        truth_cmd))
    app.add_handler(CommandHandler("dare",         dare_cmd))
    app.add_handler(CommandHandler("tod",          tod_cmd))
    app.add_handler(CommandHandler("ask",          ask_cmd))
    app.add_handler(CommandHandler("reverse",      reverse_cmd))
    app.add_handler(CommandHandler("mock",         mock_cmd))
    app.add_handler(CommandHandler("clap",         clap_cmd))
    app.add_handler(CommandHandler("aesthetic",    aesthetic_cmd))
    app.add_handler(CommandHandler("calc",         calc_cmd))
    app.add_handler(CommandHandler("typerace",     typerace_cmd))
    app.add_handler(CommandHandler("quotetxt",     quote_text_cmd))

    # ── Extra info ──────────────────────────────────────────────────────
    app.add_handler(CommandHandler("mention",      mention_cmd))
    app.add_handler(CommandHandler("userid",       userid_cmd))
    app.add_handler(CommandHandler("grouplink",    grouplink_cmd))

    # ── Personal notes ─────────────────────────────────────────────────
    app.add_handler(CommandHandler("mynote",       mynote_cmd))
    app.add_handler(CommandHandler("delmynote",    delmynote_cmd))

    # ── Report log ─────────────────────────────────────────────────────
    app.add_handler(CommandHandler("reportlog",    reportlog_cmd))

    # ── Extended help ──────────────────────────────────────────────────
    app.add_handler(CommandHandler("helptopic",    help_topic_cmd))

    # ── Member join / leave ────────────────────────────────────────────
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_member_join))
    app.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER,  on_member_left))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_and_checks))

    # ── Games ──────────────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("hangman",      hangman_cmd))
    app.add_handler(CommandHandler("guess",        guess_cmd))
    app.add_handler(CommandHandler("scramble",     scramble_cmd))
    app.add_handler(CommandHandler("unscramble",   unscramble_cmd))
    app.add_handler(CommandHandler("scramblehint", scramblehint_cmd))
    app.add_handler(CommandHandler("numguess",     numguess_cmd))
    app.add_handler(CommandHandler("ng",           ng_cmd))
    app.add_handler(CommandHandler("wordchain",    wordchain_cmd))
    app.add_handler(CommandHandler("wc",           wc_cmd))
    app.add_handler(CommandHandler("counting",     counting_cmd))
    app.add_handler(CommandHandler("emojiquiz",    emojiquiz_cmd))
    app.add_handler(CommandHandler("emojians",     emojians_cmd))
    app.add_handler(CommandHandler("moviequiz",    moviequiz_cmd))
    app.add_handler(CommandHandler("movieans",     movieans_cmd))
    app.add_handler(CommandHandler("giveaway",     giveaway_cmd))
    app.add_handler(CommandHandler("genter",       genter_cmd))
    app.add_handler(CommandHandler("gend",         gend_cmd))

    # ── Fun / Social ────────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("wyr",          wyr_cmd))
    app.add_handler(CommandHandler("nhie",         nhie_cmd))
    app.add_handler(CommandHandler("horoscope",    horoscope_cmd))
    app.add_handler(CommandHandler("f",            f_cmd))
    app.add_handler(CommandHandler("bonk",         bonk_cmd))
    app.add_handler(CommandHandler("bite",         bite_cmd))
    app.add_handler(CommandHandler("punch",        punch_cmd))
    app.add_handler(CommandHandler("poke",         poke_cmd))
    app.add_handler(CommandHandler("kiss",         kiss_cmd))
    app.add_handler(CommandHandler("marry",        marry_cmd))
    app.add_handler(CommandHandler("simp",         simp_cmd))
    app.add_handler(CommandHandler("sus",          sus_cmd))
    app.add_handler(CommandHandler("vibe",         vibe_cmd))
    app.add_handler(CommandHandler("password",     password_cmd))
    app.add_handler(CommandHandler("weather",      weather_cmd))
    app.add_handler(CommandHandler("toss",         toss_cmd))
    app.add_handler(CommandHandler("quickpoll",    quickpoll_cmd))
    app.add_handler(CommandHandler("vote",         vote_cmd))

    # ── Text tools ──────────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("uwu",          uwu_cmd))
    app.add_handler(CommandHandler("vaporwave",    vaporwave_cmd))
    app.add_handler(CommandHandler("flip",         fliptext_cmd))
    app.add_handler(CommandHandler("binary",       binary_cmd))
    app.add_handler(CommandHandler("morse",        morse_cmd))
    app.add_handler(CommandHandler("cursive",      cursive_cmd))
    app.add_handler(CommandHandler("tiny",         tiny_cmd))
    app.add_handler(CommandHandler("boldtext",     bold_txt_cmd))
    app.add_handler(CommandHandler("italic",       italic_cmd))
    app.add_handler(CommandHandler("strike",       strike_cmd))
    app.add_handler(CommandHandler("encode64",     encode64_cmd))
    app.add_handler(CommandHandler("decode64",     decode64_cmd))
    app.add_handler(CommandHandler("hash",         hash_cmd))
    app.add_handler(CommandHandler("len",          len_cmd))
    app.add_handler(CommandHandler("shout",        shout_cmd))
    app.add_handler(CommandHandler("repeat",       repeat_cmd))
    app.add_handler(CommandHandler("color",        colorhex_cmd))

    # ── Extra Rose-like features ────────────────────────────────────────────────
    app.add_handler(CommandHandler("connected",    connected_cmd))
    app.add_handler(CommandHandler("pinned",       pinned_cmd))
    app.add_handler(CommandHandler("antich",       antich_cmd))
    app.add_handler(CommandHandler("setwarnmsg",   setwarnmsg_cmd))
    app.add_handler(CommandHandler("resetwarnmsg", resetwarnmsg_cmd))
    app.add_handler(CommandHandler("antispam",     antispam_cmd))
    app.add_handler(CommandHandler("fbanlist",     fbanlist_cmd))
    app.add_handler(CommandHandler("fednotice",    fednotice_cmd))
    app.add_handler(CommandHandler("delcmd",       delcmd_cmd))
    app.add_handler(CommandHandler("globalstats",  globalstats_cmd))
    app.add_handler(CommandHandler("admins",       admins_cmd))
    app.add_handler(CommandHandler("invitelink",   invitelink_cmd))
    app.add_handler(CommandHandler("settitle",     settitle_cmd))
    app.add_handler(CommandHandler("announce",     announce_cmd))
    app.add_handler(CommandHandler("floodaction",  floodaction_cmd))
    app.add_handler(CommandHandler("stopall",      stopall_cmd))
    app.add_handler(CommandHandler("privatenote",  privatenote_cmd))
    app.add_handler(CommandHandler("userinfo",     userinfo_cmd))

    # ── Extended help ───────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("helpgames",    helpgames_cmd))
    app.add_handler(CommandHandler("helpfun",      helpfun_cmd))
    app.add_handler(CommandHandler("helptext",     helptext_cmd))
    app.add_handler(CommandHandler("helprose",     helprose_cmd))

    # All other messages
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, master_handler))

    # Callbacks
    app.add_handler(CallbackQueryHandler(callback_router))

    print("╔══════════════════════════════════════════════════╗")
    print("║      The Manager v1.0  is  running 🤖           ║")
    print("║  Captcha · Karma · Fun · Anti-Raid · +300 feats  ║")
    print("╚══════════════════════════════════════════════════╝")
    logger.info("Bot started. All handlers registered.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
