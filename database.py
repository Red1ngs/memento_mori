"""Модуль работы с базой данных."""

import logging
import json
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import aiosqlite

from timezone_utils import ts_for_db, now_msk

logger = logging.getLogger(__name__)

DB_PATH = "bot_data.db"


# ══════════════════════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════════════════════


@dataclass
class ClubCard:
    """Карта клуба."""
    id: int
    card_id: int
    card_rank: str
    card_image_url: str
    replacements: str
    daily_donated: str
    club_owners: List[int]  # список mangabuff_id
    discovered_at: str
    is_current: int


@dataclass
class User:
    """Пользователь бота."""
    id: int
    tg_id: int
    tg_username: Optional[str]
    tg_nickname: str
    mangabuff_url: str
    mangabuff_id: int
    mangabuff_nick: str
    is_active: int
    is_verified: int
    created_at: str


@dataclass
class Booking:
    """Бронь."""
    id: int
    tg_id: int
    tg_nickname: str
    mangabuff_nick: str
    date: str
    start_time: str
    end_time: str
    duration_hours: float
    status: str
    created_at: str
    confirmed_at: Optional[str]
    cancelled_at: Optional[str]
    completed_at: Optional[str]
    cancelled_by: Optional[str]
    cancel_reason: Optional[str]
    remind_sent: int
    group_notified: int


# ══════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ БД
# ══════════════════════════════════════════════════════════════


async def init_db():
    """Создаёт таблицы БД."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица карт клуба (ИСПРАВЛЕНО: убраны card_name, wants_count, owners_count)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS club_cards (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                card_id         INTEGER,
                card_rank       TEXT,
                card_image_url  TEXT,
                replacements    TEXT,
                daily_donated   TEXT,
                club_owners     TEXT,
                discovered_at   TEXT,
                is_current      INTEGER DEFAULT 1
            )
        """)
        
        # Таблица пользователей
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id           INTEGER UNIQUE NOT NULL,
                tg_username     TEXT,
                tg_nickname     TEXT,
                mangabuff_url   TEXT UNIQUE,
                mangabuff_id    INTEGER UNIQUE,
                mangabuff_nick  TEXT,
                is_active       INTEGER DEFAULT 1,
                is_verified     INTEGER DEFAULT 0,
                created_at      TEXT
            )
        """)
        
        # Таблица броней
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id               INTEGER NOT NULL,
                tg_nickname         TEXT,
                mangabuff_nick      TEXT,
                date                TEXT NOT NULL,
                start_time          TEXT NOT NULL,
                end_time            TEXT NOT NULL,
                duration_hours      REAL,
                status              TEXT DEFAULT 'pending',
                created_at          TEXT,
                confirmed_at        TEXT,
                cancelled_at        TEXT,
                completed_at        TEXT,
                cancelled_by        TEXT,
                cancel_reason       TEXT,
                remind_sent         INTEGER DEFAULT 0,
                group_notified      INTEGER DEFAULT 0,
                FOREIGN KEY (tg_id) REFERENCES users(tg_id)
            )
        """)
        
        # Таблица событий броней
        await db.execute("""
            CREATE TABLE IF NOT EXISTS booking_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id  INTEGER NOT NULL,
                event_type  TEXT NOT NULL,
                actor_tg_id INTEGER,
                actor_label TEXT,
                note        TEXT,
                event_at    TEXT NOT NULL,
                FOREIGN KEY (booking_id) REFERENCES bookings(id)
            )
        """)
        
        # Индексы
        await db.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_one_booking_per_day
            ON bookings(tg_id, date)
            WHERE status IN ('pending', 'confirmed')
        """)
        
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_bookings_date_status
            ON bookings(date, status)
        """)
        
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_bookings_tg_id
            ON bookings(tg_id, created_at DESC)
        """)
        
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_booking_id
            ON booking_events(booking_id, event_at DESC)
        """)
        
        await db.commit()
        logger.info("✅ База данных инициализирована")


# ══════════════════════════════════════════════════════════════
# КАРТЫ КЛУБА
# ══════════════════════════════════════════════════════════════


async def get_current_card() -> Optional[ClubCard]:
    """Получает текущую карту клуба."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM club_cards WHERE is_current = 1 ORDER BY id DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                row_dict = dict(row)
                # Парсим JSON для club_owners
                row_dict["club_owners"] = json.loads(row_dict["club_owners"]) if row_dict["club_owners"] else []
                return ClubCard(**row_dict)
    return None


async def insert_card(card_data: Dict[str, Any]) -> int:
    """Вставляет новую карту (ИСПРАВЛЕНО: без card_name, wants_count, owners_count)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO club_cards (
                card_id, card_rank, card_image_url,
                replacements, daily_donated, club_owners,
                discovered_at, is_current
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            card_data["card_id"],
            card_data["card_rank"],
            card_data["card_image_url"],
            card_data["replacements"],
            card_data["daily_donated"],
            json.dumps(card_data["club_owners"]),
            card_data["discovered_at"]
        ))
        await db.commit()
        return cursor.lastrowid


async def archive_card(card_id: int):
    """Архивирует карту (is_current = 0)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE club_cards SET is_current = 0 WHERE id = ?",
            (card_id,)
        )
        await db.commit()


# ══════════════════════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛИ
# ══════════════════════════════════════════════════════════════


async def get_user(tg_id: int) -> Optional[User]:
    """Получает пользователя по Telegram ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE tg_id = ?", (tg_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return User(**dict(row)) if row else None


async def get_user_by_mangabuff_id(mangabuff_id: int) -> Optional[User]:
    """Получает пользователя по MangaBuff ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE mangabuff_id = ?", (mangabuff_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return User(**dict(row)) if row else None


async def upsert_user(
    tg_id: int,
    tg_username: Optional[str],
    tg_nickname: str,
    mangabuff_url: str,
    mangabuff_id: int,
    mangabuff_nick: str,
    is_verified: int = 1,
    is_active: int = 1,
    created_at: Optional[str] = None
):
    """Создаёт или обновляет пользователя."""
    if created_at is None:
        created_at = ts_for_db(now_msk())
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (
                tg_id, tg_username, tg_nickname, mangabuff_url,
                mangabuff_id, mangabuff_nick, is_verified, is_active, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                tg_username = excluded.tg_username,
                tg_nickname = excluded.tg_nickname,
                mangabuff_url = excluded.mangabuff_url,
                mangabuff_id = excluded.mangabuff_id,
                mangabuff_nick = excluded.mangabuff_nick,
                is_verified = excluded.is_verified,
                is_active = excluded.is_active
        """, (
            tg_id, tg_username, tg_nickname, mangabuff_url,
            mangabuff_id, mangabuff_nick, is_verified, is_active, created_at
        ))
        await db.commit()


async def delete_user(tg_id: int):
    """Удаляет пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE tg_id = ?", (tg_id,))
        await db.commit()


async def toggle_user_active(tg_id: int) -> bool:
    """Переключает is_active. Возвращает новое значение."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT is_active FROM users WHERE tg_id = ?", (tg_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return False
            
            new_value = 0 if row[0] == 1 else 1
            await db.execute(
                "UPDATE users SET is_active = ? WHERE tg_id = ?",
                (new_value, tg_id)
            )
            await db.commit()
            return bool(new_value)


async def get_all_users() -> List[User]:
    """Получает всех пользователей."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users ORDER BY created_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [User(**dict(row)) for row in rows]


# ══════════════════════════════════════════════════════════════
# БРОНИРОВАНИЕ
# ══════════════════════════════════════════════════════════════


async def create_booking(
    tg_id: int,
    tg_nickname: str,
    mangabuff_nick: str,
    date: str,
    start_time: str,
    end_time: str,
    duration_hours: float
) -> int:
    """Создаёт бронь."""
    created_at = ts_for_db(now_msk())
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO bookings (
                tg_id, tg_nickname, mangabuff_nick, date,
                start_time, end_time, duration_hours,
                status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """, (
            tg_id, tg_nickname, mangabuff_nick, date,
            start_time, end_time, duration_hours, created_at
        ))
        booking_id = cursor.lastrowid
        
        # Добавляем событие
        await db.execute("""
            INSERT INTO booking_events (
                booking_id, event_type, actor_tg_id, actor_label, event_at
            ) VALUES (?, 'created', ?, 'user', ?)
        """, (booking_id, tg_id, created_at))
        
        await db.commit()
        return booking_id


async def get_booking(booking_id: int) -> Optional[Booking]:
    """Получает бронь по ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM bookings WHERE id = ?", (booking_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return Booking(**dict(row)) if row else None


async def get_user_active_bookings(tg_id: int, dates: List[str]) -> List[Booking]:
    """Получает активные брони пользователя на указанные даты."""
    placeholders = ",".join("?" * len(dates))
    query = f"""
        SELECT * FROM bookings
        WHERE tg_id = ? AND date IN ({placeholders})
          AND status IN ('pending', 'confirmed')
        ORDER BY date, start_time
    """
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, (tg_id, *dates)) as cursor:
            rows = await cursor.fetchall()
            return [Booking(**dict(row)) for row in rows]


async def get_bookings_for_schedule(dates: List[str]) -> List[Booking]:
    """Получает все активные брони на указанные даты."""
    placeholders = ",".join("?" * len(dates))
    query = f"""
        SELECT * FROM bookings
        WHERE date IN ({placeholders})
          AND status IN ('pending', 'confirmed')
        ORDER BY date, start_time
    """
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, dates) as cursor:
            rows = await cursor.fetchall()
            return [Booking(**dict(row)) for row in rows]


async def confirm_booking(booking_id: int, confirmed_at: str):
    """Подтверждает бронь."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE bookings
            SET status = 'confirmed', confirmed_at = ?
            WHERE id = ?
        """, (confirmed_at, booking_id))
        await db.commit()


async def cancel_booking(
    booking_id: int,
    cancelled_by: str,
    cancel_reason: str,
    actor_tg_id: Optional[int] = None
):
    """Отменяет бронь."""
    cancelled_at = ts_for_db(now_msk())
    status_map = {
        "user": "cancelled_by_user",
        "admin": "cancelled_by_admin",
        "system": "cancelled"
    }
    status = status_map.get(cancelled_by, "cancelled")
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE bookings
            SET status = ?, cancelled_at = ?, cancelled_by = ?, cancel_reason = ?
            WHERE id = ?
        """, (status, cancelled_at, cancelled_by, cancel_reason, booking_id))
        await db.commit()


async def complete_booking(booking_id: int, completed_at: str):
    """Завершает бронь."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE bookings
            SET status = 'completed', completed_at = ?
            WHERE id = ?
        """, (completed_at, booking_id))
        await db.commit()


async def mark_remind_sent(booking_id: int):
    """Помечает, что напоминание отправлено."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE bookings SET remind_sent = 1 WHERE id = ?",
            (booking_id,)
        )
        await db.commit()


async def mark_group_notified(booking_id: int):
    """Помечает, что группа уведомлена."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE bookings SET group_notified = 1 WHERE id = ?",
            (booking_id,)
        )
        await db.commit()


async def add_booking_event(
    booking_id: int,
    event_type: str,
    actor_label: str,
    actor_tg_id: Optional[int] = None,
    note: Optional[str] = None
):
    """Добавляет событие брони."""
    event_at = ts_for_db(now_msk())
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO booking_events (
                booking_id, event_type, actor_tg_id, actor_label, note, event_at
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (booking_id, event_type, actor_tg_id, actor_label, note, event_at))
        await db.commit()


async def get_user_booking_history(tg_id: int, limit: int = 20) -> List[Booking]:
    """Получает историю броней пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM bookings
            WHERE tg_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (tg_id, limit)) as cursor:
            rows = await cursor.fetchall()
            return [Booking(**dict(row)) for row in rows]


async def get_all_booking_history(limit: int = 50) -> List[Booking]:
    """Получает полную историю броней."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM bookings
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)) as cursor:
            rows = await cursor.fetchall()
            return [Booking(**dict(row)) for row in rows]


async def get_bookings_needing_reminder() -> List[Booking]:
    """Получает брони, которым нужно отправить напоминание."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM bookings
            WHERE status = 'pending' AND remind_sent = 0
        """) as cursor:
            rows = await cursor.fetchall()
            return [Booking(**dict(row)) for row in rows]


async def get_bookings_needing_cancellation() -> List[Booking]:
    """Получает брони, которые нужно отменить по таймауту."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM bookings
            WHERE status = 'pending' AND remind_sent = 1
        """) as cursor:
            rows = await cursor.fetchall()
            return [Booking(**dict(row)) for row in rows]


async def get_bookings_to_complete() -> List[Booking]:
    """Получает брони, которые нужно завершить."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM bookings
            WHERE status = 'confirmed'
        """) as cursor:
            rows = await cursor.fetchall()
            return [Booking(**dict(row)) for row in rows]


async def check_booking_conflict(
    date: str,
    start_time: str,
    end_time: str,
    exclude_booking_id: Optional[int] = None
) -> bool:
    """
    Проверяет конфликт броней.
    
    Returns:
        True если есть конфликт
    """
    query = """
        SELECT COUNT(*) FROM bookings
        WHERE date = ?
          AND status IN ('pending', 'confirmed')
          AND NOT (end_time <= ? OR start_time >= ?)
    """
    params = [date, start_time, end_time]
    
    if exclude_booking_id:
        query += " AND id != ?"
        params.append(exclude_booking_id)
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query, params) as cursor:
            row = await cursor.fetchone()
            return row[0] > 0