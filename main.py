#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║            The Manager — Telegram Bot v4.0              ║
║       Full-featured Group Management Bot                ║
║       Rose Bot feature-parity + all fixes               ║
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
import html
import uuid
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
BOT_TOKEN = "8879135962:AAHTdGaJnNwDuoWqDVSmMn7Jt6hg5lhqc-U"
OWNER_ID  = 6336459877
MONGO_URI = "mongodb+srv://eclbot:eclbot1234@cluster0.eamckjk.mongodb.net/?appName=Cluster0"

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


def init_mongo():
    global mongo_users_col, mongo_gbans_col
    if not MONGO_AVAILABLE:
        logger.warning("pymongo not installed. pip install pymongo dnspython")
        return
    if not MONGO_URI:
        return
    try:
        import dns.resolver
        dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
        dns.resolver.default_resolver.nameservers = ["8.8.8.8", "8.8.4.4", "1.1.1.1"]
    except Exception:
        pass
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        client.server_info()
        db = client["themanager"]
        mongo_users_col = db["users"]
        mongo_gbans_col = db["gbans"]
        # Create indexes on both possible field names so old data works too
        try:
            mongo_users_col.create_index("username", sparse=True)
        except Exception:
            pass
        try:
            mongo_users_col.create_index("user_id", unique=True, sparse=True)
        except Exception:
            pass
        try:
            mongo_users_col.create_index("id", sparse=True)
        except Exception:
            pass
        try:
            mongo_gbans_col.create_index("user_id", unique=True, sparse=True)
        except Exception:
            pass
        logger.info("✅ MongoDB connected!")
    except Exception as e:
        logger.warning(f"MongoDB connection failed: {e}")
        mongo_users_col = None


def _mongo_uid(doc) -> int | None:
    """Extract user_id from a mongo doc that may use 'user_id' or 'id'."""
    return doc.get("user_id") or doc.get("id")


def _mongo_fname(doc) -> str:
    """Extract first_name from a mongo doc that may use different keys."""
    return doc.get("first_name") or doc.get("name") or doc.get("fname") or "Unknown"


def _mongo_uname(doc) -> str | None:
    return doc.get("username") or doc.get("user_name")


def mongo_upsert_user(user) -> None:
    if mongo_users_col is None:
        return
    try:
        uname = getattr(user, "username", None)
        doc = {
            "user_id":    user.id,
            "id":         user.id,
            "first_name": user.first_name or "Unknown",
            "last_name":  getattr(user, "last_name", None),
            "username":   uname.lower() if uname else None,
            "last_seen":  int(time.time()),
        }
        mongo_users_col.update_one({"user_id": user.id}, {"$set": doc}, upsert=True)
    except Exception:
        pass


def mongo_find_by_username(username: str):
    if mongo_users_col is None:
        return None
    clean = username.lower().lstrip("@")
    try:
        doc = mongo_users_col.find_one({"username": clean})
        if doc:
            return doc
        # Fallback: also try user_name field
        doc = mongo_users_col.find_one({"user_name": clean})
        return doc
    except Exception:
        return None


def mongo_find_by_id(user_id: int):
    if mongo_users_col is None:
        return None
    try:
        doc = mongo_users_col.find_one({"user_id": user_id})
        if doc:
            return doc
        doc = mongo_users_col.find_one({"id": user_id})
        return doc
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
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("🔒 This command is for the bot owner only!")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def mention(user) -> str:
    name = html.escape(str(user.first_name or "Unknown"))
    return f'<a href="tg://user?id={user.id}">{name}</a>'


def cache_user(user) -> None:
    """Cache user data in SQLite (fast local lookup) + MongoDB (persists across redeploys)."""
    if not user or getattr(user, "is_bot", False):
        return
    uname = getattr(user, "username", None)
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO user_cache (user_id, first_name, last_name, username, last_seen) "
            "VALUES (?,?,?,?,?)",
            (user.id, user.first_name, getattr(user, "last_name", None),
             uname.lower() if uname else None, int(time.time())),
        )
        conn.commit()
    except Exception:
        pass
    mongo_upsert_user(user)


def record_chat_member(chat_id: int, user) -> None:
    """Record that this user is (or was recently) a member of chat_id."""
    if not user or getattr(user, "is_bot", False):
        return
    uname = getattr(user, "username", None)
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO chat_members (chat_id, user_id, first_name, username, last_seen) "
            "VALUES (?,?,?,?,?)",
            (chat_id, user.id, user.first_name or "Unknown",
             uname.lower() if uname else None, int(time.time())),
        )
        conn.commit()
    except Exception:
        pass


def sqlite_find_by_username(username: str):
    clean = username.lower().lstrip("@")
    cursor.execute(
        "SELECT user_id, first_name, last_name, username FROM user_cache WHERE username=?", (clean,)
    )
    return cursor.fetchone()


def sqlite_find_by_id(user_id: int):
    cursor.execute(
        "SELECT user_id, first_name, last_name, username FROM user_cache WHERE user_id=?", (user_id,)
    )
    return cursor.fetchone()


class _CachedUser:
    """Minimal user-like object built from DB rows."""
    def __init__(self, user_id, first_name="Unknown", last_name=None, username=None):
        self.id         = int(user_id)
        self.first_name = first_name or "Unknown"
        self.last_name  = last_name
        self.username   = username
        self.is_bot     = False


async def get_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    3-tier user resolution:
      1. Replied-to message
      2. Numeric ID  → Telegram API → SQLite → MongoDB
      3. @username   → Telegram API → SQLite → MongoDB (tries both field name variations)
    """
    if update.message and update.message.reply_to_message:
        u = update.message.reply_to_message.from_user
        if u:
            return u, None

    if not context.args:
        return None, "❌ Reply to a user's message or provide their @username / ID."

    raw = context.args[0].lstrip("@")

    # ── Numeric ID ──────────────────────────────────────
    if raw.isdigit():
        uid = int(raw)
        try:
            user = await context.bot.get_chat(uid)
            return user, None
        except (BadRequest, TelegramError):
            pass
        row = sqlite_find_by_id(uid)
        if row:
            return _CachedUser(*row), None
        doc = mongo_find_by_id(uid)
        if doc:
            return _CachedUser(_mongo_uid(doc), _mongo_fname(doc), doc.get("last_name"), _mongo_uname(doc)), None
        return None, (
            f"❌ User <code>{uid}</code> not found in any cache.\n"
            "Have them send a message in the group first, or give me their username."
        )

    # ── @username ────────────────────────────────────────
    try:
        user = await context.bot.get_chat(f"@{raw}")
        return user, None
    except (BadRequest, TelegramError):
        pass

    row = sqlite_find_by_username(raw)
    if row:
        return _CachedUser(*row), None

    doc = mongo_find_by_username(raw)
    if doc:
        return _CachedUser(_mongo_uid(doc), _mongo_fname(doc), doc.get("last_name"), _mongo_uname(doc)), None

    return (
        None,
        f"❌ @{raw} not found in Telegram API, local cache, or MongoDB.\n"
        "💡 They must send at least one message in a monitored group.\n"
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


def get_stat(key: str) -> int:
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
        cache_user(user)
        # Track DM users
        try:
            cursor.execute(
                "INSERT OR REPLACE INTO dm_users (user_id, first_name, username, started_at) VALUES (?,?,?,?)",
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
        "👋 <b>Hey! I'm The Manager v4.0!</b> 🤖\n\n"
        "Your ultimate Telegram group management companion!\n\n"
        "✨ <b>New in v4:</b>\n"
        "• <b>@all</b> — tag all members 10 at a time with emojis\n"
        "• <b>/adminlock</b> — lock content types even for admins\n"
        "• <b>/lock all</b> / <b>/unlock all</b> — lock/unlock everything at once\n"
        "• Filters now trigger on images/stickers/gifs too\n"
        "• Fixed bold text in all messages\n"
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
        await update.message.reply_text(
            f"{msg}\n\n👤 {mention(target)}\n📋 Reason: <i>{html.escape(reason)}</i>",
            parse_mode=ParseMode.HTML,
        )
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
        await update.message.reply_text(f"{msg}\n\n👤 {mention(target)}", parse_mode=ParseMode.HTML)
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
        await update.message.reply_text(f"{msg}\n\n👤 {mention(target)}", parse_mode=ParseMode.HTML)
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
        return await update.message.reply_text("❌ Provide a time! E.g. /tmute @user 30m")
    until = datetime.now() + timedelta(seconds=secs)
    try:
        await context.bot.restrict_chat_member(
            update.effective_chat.id, target.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until,
        )
        context.application.job_queue.run_once(
            _do_unmute, when=secs,
            data=(update.effective_chat.id, target.id, target.first_name),
        )
        await update.message.reply_text(
            f"🔇 {mention(target)} muted for <b>{raw}</b>!\n"
            f"<i>Auto-unmutes at {until.strftime('%H:%M:%S')}</i>",
            parse_mode=ParseMode.HTML,
        )
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
                notice = await context.bot.send_message(
                    chat_id,
                    f"🔐 <b>Admin lock</b>: <b>{lock_type}</b> is restricted in this group "
                    f"(applies to admins too).",
                    parse_mode=ParseMode.HTML,
                )
                await asyncio.sleep(4)
                try:
                    await notice.delete()
                except BadRequest:
                    pass
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

    await update.message.reply_text(
        f"🤖 <b>Bot Stats (Owner View)</b>\n\n"
        f"⏱️ Uptime: <b>{fmt_secs(uptime)}</b>\n\n"
        f"👥 <b>User Data:</b>\n"
        f"• SQLite cache: <b>{sqlite_users}</b> users\n"
        f"• MongoDB: <b>{mongo_user_count}</b> users\n"
        f"• Started DM: <b>{dm_count}</b> users\n\n"
        f"🏘️ <b>Groups:</b> <b>{group_count}</b>\n\n"
        f"🌍 <b>Global Bans:</b> <b>{gban_count}</b>\n\n"
        f"<b>Session Actions:</b>\n"
        f"🔨 Bans: {get_stat('bans')} | 👢 Kicks: {get_stat('kicks')}\n"
        f"🔇 Mutes: {get_stat('mutes')} | ⚠️ Warns: {get_stat('warns')}",
        parse_mode=ParseMode.HTML,
    )


@owner_only
async def botgroups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    cursor.execute("SELECT DISTINCT chat_id FROM welcome")
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
#  MASTER MESSAGE HANDLER
# ══════════════════════════════════════════════════════════
async def master_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    # Auto-cache user on every message
    cache_user(update.effective_user)

    # Track the group and record per-group membership
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

    await check_afk(update, context)
    await check_blacklist(update, context)
    await enforce_locks(update, context)
    await enforce_admin_locks(update, context)
    await check_flood(update, context)
    await process_filters(update, context)
    await check_hashtag_note(update, context)


# ══════════════════════════════════════════════════════════
#  APPLICATION SETUP
# ══════════════════════════════════════════════════════════
def main():
    init_mongo()
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

    # Bot stats / groups (owner)
    app.add_handler(CommandHandler("botstats",  botstats_cmd))
    app.add_handler(CommandHandler("botgroups", botgroups_cmd))

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

    # Member join / leave
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_member_join))
    app.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER,  on_member_left))

    # All other messages
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, master_handler))

    # Callbacks
    app.add_handler(CallbackQueryHandler(callback_router))

    print("╔══════════════════════════════════════════════════╗")
    print("║       The Manager v4.0  is  running 🤖         ║")
    print("║   All fixes applied + @all + adminlock + more   ║")
    print("╚══════════════════════════════════════════════════╝")
    logger.info("Bot started. All handlers registered.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
