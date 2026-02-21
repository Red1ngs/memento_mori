"""
Модуль недельной статистики вкладов в клуб.
"""

import hashlib
import logging
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import aiosqlite
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import TelegramError

from config import BASE_URL, REQUIRED_TG_GROUP_ID, GROUP_CARD_TOPIC_ID
from timezone_utils import now_msk, ts_for_db

logger = logging.getLogger(__name__)
DB_PATH = "bot_data.db"


# ══════════════════════════════════════════════════════════════
# УТИЛИТЫ НЕДЕЛИ
# ══════════════════════════════════════════════════════════════


def get_week_start(dt: datetime = None) -> str:
    if dt is None:
        dt = now_msk()
    monday = dt.date() - timedelta(days=dt.weekday())
    return monday.isoformat()


def get_week_end(week_start: str) -> str:
    monday = datetime.strptime(week_start, "%Y-%m-%d").date()
    return (monday + timedelta(days=6)).isoformat()


def format_week_range(week_start: str) -> str:
    week_end = get_week_end(week_start)
    s = datetime.strptime(week_start, "%Y-%m-%d")
    e = datetime.strptime(week_end, "%Y-%m-%d")
    return f"{s.day:02d}.{s.month:02d} — {e.day:02d}.{e.month:02d}"


# ══════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ
# ══════════════════════════════════════════════════════════════


async def ensure_weekly_tables():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS weekly_contributions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start      TEXT NOT NULL,
                mangabuff_id    INTEGER NOT NULL,
                nick            TEXT NOT NULL,
                profile_url     TEXT,
                contribution    INTEGER NOT NULL DEFAULT 0,
                recorded_at     TEXT NOT NULL,
                UNIQUE(week_start, mangabuff_id)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_weekly_week_start
            ON weekly_contributions(week_start, contribution DESC)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pinned_weekly_message (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER NOT NULL UNIQUE,
                thread_id   INTEGER,
                message_id  INTEGER NOT NULL,
                week_start  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            )
        """)
        await db.commit()


# ══════════════════════════════════════════════════════════════
# ПАРСИНГ HTML
# ══════════════════════════════════════════════════════════════


def parse_weekly_contributions(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(".club-boost__top-item")
    if not items:
        logger.warning("Не найдены .club-boost__top-item в ответе недельной статистики")
        return []

    results = []
    for item in items:
        pos_el = item.select_one(".club-boost__top-position")
        try:
            position = int(pos_el.text.strip()) if pos_el else 0
        except ValueError:
            position = 0

        name_link = item.select_one("a.club-boost__top-name")
        if not name_link:
            continue

        nick = name_link.text.strip()
        href = name_link.get("href", "")

        match = re.search(r"/users/(\d+)", href)
        mangabuff_id = int(match.group(1)) if match else 0

        profile_url = f"{BASE_URL}{href}" if href.startswith("/") else href

        contrib_el = item.select_one(".club-boost__top-contribution")
        try:
            contribution = int(contrib_el.text.strip()) if contrib_el else 0
        except ValueError:
            contribution = 0

        results.append({
            "position":     position,
            "mangabuff_id": mangabuff_id,
            "nick":         nick,
            "profile_url":  profile_url,
            "contribution": contribution,
        })

    logger.debug(f"Спарсено {len(results)} участников из недельной статистики")
    return results


def compute_stats_hash(contributions: List[Dict]) -> str:
    data = ",".join(
        f"{c['mangabuff_id']}:{c['contribution']}"
        for c in contributions
    )
    return hashlib.md5(data.encode()).hexdigest()


# ══════════════════════════════════════════════════════════════
# РАБОТА С БД
# ══════════════════════════════════════════════════════════════


async def save_weekly_contributions(week_start: str, contributions: List[Dict]):
    await ensure_weekly_tables()
    recorded_at = ts_for_db(now_msk())

    async with aiosqlite.connect(DB_PATH) as db:
        for c in contributions:
            await db.execute("""
                INSERT INTO weekly_contributions
                    (week_start, mangabuff_id, nick, profile_url, contribution, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(week_start, mangabuff_id) DO UPDATE SET
                    nick         = excluded.nick,
                    contribution = excluded.contribution,
                    recorded_at  = excluded.recorded_at
            """, (
                week_start,
                c["mangabuff_id"],
                c["nick"],
                c["profile_url"],
                c["contribution"],
                recorded_at,
            ))
        await db.commit()

    logger.debug(f"Сохранено {len(contributions)} записей за неделю {week_start}")


async def get_week_contributions_from_db(week_start: str) -> List[Dict]:
    await ensure_weekly_tables()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM weekly_contributions
            WHERE week_start = ?
            ORDER BY contribution DESC
        """, (week_start,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_available_weeks() -> List[str]:
    await ensure_weekly_tables()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT DISTINCT week_start
            FROM weekly_contributions
            ORDER BY week_start DESC
        """) as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]


# ══════════════════════════════════════════════════════════════
# ЗАКРЕПЛЁННОЕ СООБЩЕНИЕ — БД
# ══════════════════════════════════════════════════════════════


async def get_pinned_message_info(chat_id: int) -> Optional[Dict]:
    await ensure_weekly_tables()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM pinned_weekly_message WHERE chat_id = ?",
            (chat_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def save_pinned_message_info(
    chat_id: int,
    thread_id: Optional[int],
    message_id: int,
    week_start: str,
):
    await ensure_weekly_tables()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO pinned_weekly_message
                (chat_id, thread_id, message_id, week_start, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                thread_id  = excluded.thread_id,
                message_id = excluded.message_id,
                week_start = excluded.week_start,
                updated_at = excluded.updated_at
        """, (chat_id, thread_id, message_id, week_start, ts_for_db(now_msk())))
        await db.commit()


async def clear_pinned_message_info(chat_id: int):
    await ensure_weekly_tables()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM pinned_weekly_message WHERE chat_id = ?",
            (chat_id,)
        )
        await db.commit()


# ══════════════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ СООБЩЕНИЯ
# ══════════════════════════════════════════════════════════════


def _plural_contribution(n: int) -> str:
    """Склонение: 1 вклад, 2 вклада, 5 вкладов."""
    if 11 <= (n % 100) <= 14:
        return "вкладов"
    last = n % 10
    if last == 1:
        return "вклад"
    if last in (2, 3, 4):
        return "вклада"
    return "вкладов"


def format_weekly_message(contributions: List[Dict], week_start: str) -> str:
    date_range = format_week_range(week_start)

    if not contributions:
        return (
            f"📊 <b>Топ вкладчиков недели</b> ({date_range})\n\n"
            f"Пока никто не сделал вклад."
        )

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = []

    for i, c in enumerate(contributions, 1):
        prefix = medals.get(i, f"<b>{i}.</b>")
        nick   = c["nick"]
        url    = c.get("profile_url", "")
        count  = c["contribution"]
        word   = _plural_contribution(count)

        name_part = f'<a href="{url}">{nick}</a>' if url else nick
        lines.append(f"{prefix} {name_part} — {count} {word}")

    updated = now_msk().strftime("%d.%m %H:%M МСК")

    return (
        f"📊 <b>Топ вкладчиков недели</b> ({date_range})\n\n"
        + "\n".join(lines)
        + f"\n\n🕐 <i>Обновлено: {updated}</i>"
    )


# ══════════════════════════════════════════════════════════════
# ОТПРАВКА И ОБНОВЛЕНИЕ ЗАКРЕПЛЁННОГО СООБЩЕНИЯ
# ══════════════════════════════════════════════════════════════


async def send_or_update_weekly_pinned(
    bot: Bot,
    contributions: List[Dict],
    week_start: str,
):
    chat_id   = REQUIRED_TG_GROUP_ID
    thread_id = GROUP_CARD_TOPIC_ID
    text      = format_weekly_message(contributions, week_start)

    pinned_info = await get_pinned_message_info(chat_id)

    if pinned_info and pinned_info.get("week_start") != week_start:
        logger.info(f"🔄 Смена недели → создаём новое закреплённое сообщение")
        pinned_info = None

    if pinned_info:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=pinned_info["message_id"],
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            await save_pinned_message_info(
                chat_id, thread_id, pinned_info["message_id"], week_start
            )
            logger.info("✅ Закреплённое сообщение недельной статистики обновлено")
            return

        except TelegramError as e:
            err = str(e).lower()
            if "message to edit not found" in err or "message_id_invalid" in err:
                logger.warning("Закреплённое сообщение удалено, создаём новое")
            elif "message is not modified" in err:
                logger.debug("Закреплённое сообщение не изменилось, пропускаем")
                return
            else:
                logger.error(f"Ошибка редактирования закреплённого сообщения: {e}")
                return

    try:
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            message_thread_id=thread_id,
            disable_web_page_preview=True,
        )
        try:
            await bot.pin_chat_message(
                chat_id=chat_id,
                message_id=msg.message_id,
                disable_notification=True,
            )
            logger.info("📌 Новое сообщение статистики закреплено")
        except TelegramError as e:
            logger.warning(
                f"Не удалось закрепить сообщение: {e}\n"
                "Убедись что бот — администратор с правом 'Закреплять сообщения'"
            )

        await save_pinned_message_info(chat_id, thread_id, msg.message_id, week_start)
        logger.info("✅ Новое закреплённое сообщение недельной статистики отправлено")

    except TelegramError as e:
        logger.error(f"Ошибка отправки недельной статистики: {e}")