"""
Общие утилиты для процесса бронирования.

Выделены из booking.py и group_booking.py для устранения дублирования.
"""

import logging
from typing import Optional, Tuple, List

from database import get_user, get_user_active_bookings, User, Booking
from timezone_utils import get_today_date, get_tomorrow_date, format_date_ru

logger = logging.getLogger(__name__)


async def get_verified_user(tg_id: int) -> Optional[User]:
    """
    Возвращает верифицированного пользователя или None.

    Args:
        tg_id: Telegram ID пользователя

    Returns:
        User если верифицирован, иначе None
    """
    user = await get_user(tg_id)
    if not user or not user.is_verified:
        return None
    return user


async def get_active_bookings_today_tomorrow(tg_id: int) -> List[Booking]:
    """
    Возвращает активные брони пользователя на сегодня и завтра.

    Args:
        tg_id: Telegram ID пользователя

    Returns:
        список активных броней
    """
    today = get_today_date()
    tomorrow = get_tomorrow_date()
    return await get_user_active_bookings(tg_id, [today, tomorrow])


def format_active_bookings_text(bookings: List[Booking], for_group: bool = False) -> str:
    """
    Форматирует текст сообщения об активных бронях.

    Args:
        bookings: список активных броней
        for_group: True если сообщение для группового чата

    Returns:
        текст сообщения
    """
    text = "📋 У тебя уже есть активные брони:\n\n"
    for b in bookings:
        status_emoji = "🟢" if b.status == "confirmed" else "🟡"
        text += (
            f"{status_emoji} {format_date_ru(b.date)} | "
            f"🕐 {b.start_time} — {b.end_time} МСК\n"
            f"Для отмены брони используй команду: /cancelbooking {b.id}\n"
        )

    if for_group:
        text += "\n⚠️ Одна дата — одна бронь."
    else:
        text += "\nОдна дата — одна бронь. Хочешь отменить? → /cancelbooking"

    return text