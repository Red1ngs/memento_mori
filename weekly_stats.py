"""
Модуль недельной статистики вкладов в клуб.

Парсит блок data-page="week" со страницы буста,
хранит историю по неделям в БД,
ведёт закреплённое сообщение в топике карт.
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
    """Возвращает дату понедельника текущей недели (YYYY-MM-DD)."""
    if dt is None:
        dt = now_msk()
    monday = dt.date() - timedelta(days=dt.weekday())
    return monday.isoformat()


def get_week_end(week_start: str) -> str:
    """Возвращает дату воскресенья недели (YYYY-MM-DD)."""
    monday = datetime.strptime(week_start, "%Y-%m-%d").date()
    return (monday + timedelta(days=6)).isoformat()


def format_week_range(week_start: str) -> str:
    """Форматирует диапазон дат недели: 'дд.мм — дд.мм'."""
    week_end = get_week_end(week_start)
    s = datetime.strptime(week_start, "%Y-%m-%d")
    e = datetime.strptime(week_end, "%Y-%m-%d")
    return f"{s.day:02d}.{s.month:02d} — {e.day:02d}.{e.month:02d}"


# ══════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ
# ══════════════════════════════════════════════════════════════


async def ensure_weekly_tables():
    """Создаёт таблицы недельной статистики, если их нет."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица вкладов по неделям
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

        # Таблица хранения message_id закреплённого сообщения
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
    """
    Парсит недельную статистику вкладов из AJAX-ответа клуба.

    Вкладка «Неделя» подгружается через AJAX:
        GET /clubs/getTopUsers?period=week
    Ответ содержит HTML с .club-boost__top-item напрямую,
    без обёртки data-page="week".

    Args:
        html: HTML из AJAX-ответа /clubs/getTopUsers?period=week

    Returns:
        список словарей с данными участников
    """
    soup = BeautifulSoup(html, "html.parser")

    # AJAX-ответ — ищем элементы напрямую в любом месте документа
    items = soup.select(".club-boost__top-item")
    if not items:
        logger.warning("Не найдены .club-boost__top-item в ответе недельной статистики клуба")
        return []

    results = []

    for item in items:
        # Позиция
        pos_el = item.select_one(".club-boost__top-position")
        try:
            position = int(pos_el.text.strip()) if pos_el else 0
        except ValueError:
            position = 0

        # Ник и ссылка на профиль
        name_link = item.select_one("a.club-boost__top-name")
        if not name_link:
            continue

        nick = name_link.text.strip()
        href = name_link.get("href", "")

        # Извлекаем mangabuff_id из href вида /users/12345
        match = re.search(r"/users/(\d+)", href)
        mangabuff_id = int(match.group(1)) if match else 0

        if href.startswith("/"):
            profile_url = f"{BASE_URL}{href}"
        else:
            profile_url = href

        # Количество вкладов
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
    """Вычисляет MD5-хэш списка вкладов для детектирования изменений."""
    data = ",".join(
        f"{c['mangabuff_id']}:{c['contribution']}"
        for c in contributions
    )
    return hashlib.md5(data.encode()).hexdigest()


# ══════════════════════════════════════════════════════════════
# РАБОТА С БД
# ══════════════════════════════════════════════════════════════


async def save_weekly_contributions(week_start: str, contributions: List[Dict]):
    """
    Сохраняет / обновляет вклады недели в БД.

    При конфликте по (week_start, mangabuff_id) обновляет contribution и ник.
    """
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
    """Возвращает вклады за указанную неделю из БД (сортировка по убыванию)."""
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
    """Возвращает список недель (week_start), для которых есть данные в БД."""
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
# ЗАКРЕПЛЁННОЕ СООБЩЕНИЕ
# ══════════════════════════════════════════════════════════════


async def get_pinned_message_info(chat_id: int) -> Optional[Dict]:
    """Возвращает информацию о закреплённом сообщении для чата."""
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
    """Сохраняет / обновляет информацию о закреплённом сообщении."""
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
    """Удаляет запись о закреплённом сообщении (при смене недели)."""
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


def format_weekly_message(contributions: List[Dict], week_start: str) -> str:
    """
    Форматирует текст закреплённого сообщения недельной статистики.

    Args:
        contributions: список вкладчиков (отсортирован по убыванию)
        week_start:    дата понедельника недели

    Returns:
        HTML-текст сообщения
    """
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
        nick = c["nick"]
        url = c.get("profile_url", "")
        count = c["contribution"]

        name_part = f'<a href="{url}">{nick}</a>' if url else nick
        lines.append(f"{prefix} {name_part} — {count}")

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
    """
    Отправляет или редактирует закреплённое сообщение недельной статистики.

    Логика:
    1. Получаем из БД сохранённый message_id для этого чата.
    2. Если есть — пытаемся edit_message_text.
    3. Если сообщение не найдено (удалено) — отправляем новое и закрепляем.
    4. При смене недели (week_start изменился) — отправляем новое сообщение.

    Args:
        bot:           экземпляр Telegram бота
        contributions: список вкладчиков текущей недели
        week_start:    дата понедельника (YYYY-MM-DD)
    """
    chat_id = REQUIRED_TG_GROUP_ID
    thread_id = GROUP_CARD_TOPIC_ID

    text = format_weekly_message(contributions, week_start)
    pinned_info = await get_pinned_message_info(chat_id)

    # Проверяем: сообщение принадлежит текущей неделе?
    if pinned_info and pinned_info.get("week_start") != week_start:
        logger.info(
            f"🔄 Смена недели: {pinned_info['week_start']} → {week_start}, "
            f"создаём новое закреплённое сообщение"
        )
        pinned_info = None  # Отправим новое

    if pinned_info:
        # Пробуем отредактировать существующее сообщение
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=pinned_info["message_id"],
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            # Обновляем updated_at в БД
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
                # Текст не изменился — ничего делать не надо
                logger.debug("Закреплённое сообщение не изменилось, пропускаем")
                return
            else:
                logger.error(f"Ошибка редактирования закреплённого сообщения: {e}")
                return

    # Отправляем новое сообщение
    try:
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            message_thread_id=thread_id,
            disable_web_page_preview=True,
        )

        # Пробуем закрепить
        try:
            await bot.pin_chat_message(
                chat_id=chat_id,
                message_id=msg.message_id,
                disable_notification=True,
            )
            logger.info("📌 Новое сообщение статистики закреплено")
        except TelegramError as e:
            logger.warning(f"Не удалось закрепить сообщение (нет прав?): {e}")

        await save_pinned_message_info(chat_id, thread_id, msg.message_id, week_start)
        logger.info("✅ Новое закреплённое сообщение недельной статистики отправлено")

    except TelegramError as e:
        logger.error(f"Ошибка отправки недельной статистики: {e}")