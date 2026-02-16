"""Валидация и расчёт слотов бронирования."""

import logging
import math
from datetime import datetime, timedelta
from typing import List, Tuple

from config import BOOKING_MAX_HOURS
from timezone_utils import now_msk, parse_booking_dt
from database import Booking, check_booking_conflict

logger = logging.getLogger(__name__)

# Максимум слотов в inline-клавиатуре (ограничение UX)
MAX_INLINE_SLOTS = 20


# ══════════════════════════════════════════════════════════════
# РАСЧЁТ СЛОТОВ
# ══════════════════════════════════════════════════════════════


def get_next_half_hour(now: datetime) -> int:
    """
    Возвращает количество минут от полуночи до ближайшего
    будущего получасия.

    Args:
        now: текущее время

    Returns:
        минуты от полуночи (14:24 → 870 = 14*60+30)
    """
    minutes = now.hour * 60 + now.minute
    return math.ceil((minutes + 1) / 30) * 30


def _slot_overlaps_bookings(slot_time: str, bookings: List[Booking]) -> bool:
    """
    Проверяет, попадает ли слот внутрь одной из существующих броней.

    Начинать бронь нельзя, если slot_time >= start AND slot_time < end.

    Args:
        slot_time: время в формате HH:MM
        bookings: активные брони на эту дату

    Returns:
        True если слот занят
    """
    for b in bookings:
        if b.start_time <= slot_time < b.end_time:
            return True
    return False


def get_available_start_slots(
    selected_date: str,
    busy_bookings: List[Booking]
) -> List[str]:
    """
    Возвращает доступные слоты начала брони.

    Слот недоступен если он попадает внутрь любой из занятых броней
    (start <= slot < end), а не только если совпадает с чьим-то началом.

    Args:
        selected_date: дата в формате YYYY-MM-DD
        busy_bookings: список активных броней на эту дату

    Returns:
        список времён в формате HH:MM
    """
    now = now_msk()
    today = now.date().isoformat()

    # Определяем начальный слот
    if selected_date == today:
        start_min = get_next_half_hour(now)
    else:
        start_min = 0

    slots = []
    for m in range(start_min, 24 * 60, 30):
        h, mn = divmod(m, 60)
        slot = f"{h:02d}:{mn:02d}"

        if not _slot_overlaps_bookings(slot, busy_bookings):
            slots.append(slot)

    return slots


def get_available_end_slots(
    date: str,
    start_time: str,
    busy_bookings: List[Booking]
) -> List[str]:
    """
    Возвращает доступные слоты окончания брони.

    Правила:
    1. end_time > start_time
    2. end_time <= start_time + BOOKING_MAX_HOURS
    3. Останавливается на первом конфликте с чужой бронью

    Args:
        date: дата брони
        start_time: время начала
        busy_bookings: список активных броней на эту дату

    Returns:
        список времён в формате HH:MM
    """
    start_dt = parse_booking_dt(date, start_time)
    max_end = start_dt + timedelta(hours=BOOKING_MAX_HOURS)

    slots = []
    for delta in range(30, BOOKING_MAX_HOURS * 60 + 1, 30):
        candidate_dt = start_dt + timedelta(minutes=delta)

        # Проверяем максимальную длительность
        if candidate_dt > max_end:
            break

        candidate_time = candidate_dt.strftime("%H:%M")

        # Проверяем конфликт с существующими бронями
        if has_conflict_with_bookings(start_time, candidate_time, busy_bookings):
            break  # Останавливаемся на первом конфликте

        slots.append(candidate_time)

    return slots


def has_conflict_with_bookings(
    start_time: str,
    end_time: str,
    bookings: List[Booking]
) -> bool:
    """
    Проверяет конфликт с существующими бронями.

    Args:
        start_time: время начала новой брони
        end_time: время окончания новой брони
        bookings: список активных броней

    Returns:
        True если есть конфликт
    """
    for booking in bookings:
        # Конфликт если интервалы пересекаются
        if not (end_time <= booking.start_time or start_time >= booking.end_time):
            return True
    return False


# ══════════════════════════════════════════════════════════════
# ВАЛИДАЦИЯ
# ══════════════════════════════════════════════════════════════


async def validate_booking_slot(
    date: str,
    start_time: str,
    end_time: str,
    exclude_booking_id: int = None
) -> Tuple[bool, str]:
    """
    Финальная валидация слота перед созданием брони.

    Args:
        date: дата брони
        start_time: время начала
        end_time: время окончания
        exclude_booking_id: ID брони для исключения (при редактировании)

    Returns:
        (is_valid, error_message)
    """
    # Проверка 1: end_time > start_time
    if end_time <= start_time:
        return False, "Время окончания должно быть позже времени начала"

    # Проверка 2: длительность <= BOOKING_MAX_HOURS
    start_dt = parse_booking_dt(date, start_time)
    end_dt = parse_booking_dt(date, end_time)
    duration = (end_dt - start_dt).total_seconds() / 3600

    if duration > BOOKING_MAX_HOURS:
        return False, f"Максимальная длительность брони: {BOOKING_MAX_HOURS} ч"

    # Проверка 3: нет конфликта с другими бронями (race condition guard)
    has_conflict = await check_booking_conflict(
        date, start_time, end_time, exclude_booking_id
    )

    if has_conflict:
        return False, "Этот слот уже занят"

    return True, ""


def format_time_slots_keyboard(slots: List[str], per_row: int = 4) -> List[List[str]]:
    """
    Форматирует слоты времени для клавиатуры.

    Args:
        slots: список времён ["14:30", "15:00", ...]
        per_row: количество кнопок в ряду

    Returns:
        список рядов кнопок
    """
    keyboard = []
    for i in range(0, len(slots), per_row):
        row = slots[i:i + per_row]
        keyboard.append(row)
    return keyboard