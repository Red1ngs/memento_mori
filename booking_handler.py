"""Обработчик триггера бронирования и подтверждения."""

import logging
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CallbackQueryHandler

from database import get_booking, confirm_booking, add_booking_event
from timezone_utils import ts_for_db, now_msk, format_date_ru, format_time_range
from booking import start_booking_flow

logger = logging.getLogger(__name__)

# Regex для триггера бронирования
BOOKING_TRIGGER = re.compile(
    r'\b(бронь|забронировать|бронировать)\b',
    re.IGNORECASE
)


# ══════════════════════════════════════════════════════════════
# ТРИГГЕР БРОНИРОВАНИЯ
# ══════════════════════════════════════════════════════════════


async def booking_trigger_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик триггерных слов для бронирования.
    
    Вызывает start_booking_flow из booking.py
    """
    return await start_booking_flow(update, context)


# ══════════════════════════════════════════════════════════════
# ПОДТВЕРЖДЕНИЕ БРОНИ
# ══════════════════════════════════════════════════════════════


async def confirm_booking_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback для подтверждения брони."""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем booking_id из callback_data
    # Формат: "confirm_booking:123"
    try:
        booking_id = int(query.data.split(":")[1])
    except (IndexError, ValueError):
        await query.edit_message_text("❌ Ошибка: неверный формат данных")
        return
    
    # Получаем бронь
    booking = await get_booking(booking_id)
    
    if not booking:
        await query.edit_message_text("❌ Бронь не найдена")
        return
    
    # Проверяем статус
    if booking.status != "pending":
        status_text = {
            "confirmed": "уже подтверждена",
            "cancelled": "отменена",
            "cancelled_by_user": "отменена",
            "cancelled_by_admin": "отменена администратором",
            "completed": "завершена"
        }.get(booking.status, "неактивна")
        
        await query.edit_message_text(f"❌ Бронь {status_text}")
        return
    
    # Подтверждаем бронь
    confirmed_at = ts_for_db(now_msk())
    await confirm_booking(booking_id, confirmed_at)
    await add_booking_event(
        booking_id,
        "confirmed",
        "user",
        actor_tg_id=query.from_user.id
    )
    
    # Обновляем сообщение
    await query.edit_message_text(
        f"✅ Бронь подтверждена!\n\n"
        f"🃏 Внос карт в клуб \n"
        f"📅 {format_date_ru(booking.date)}\n"
        f"🕐 {format_time_range(booking.start_time, booking.end_time)}\n\n"
        f"Удачного вноса! 🚀"
    )
    
    logger.info(f"✅ Бронь #{booking_id} подтверждена пользователем {query.from_user.id}")


# ══════════════════════════════════════════════════════════════
# HANDLERS
# ══════════════════════════════════════════════════════════════


def get_confirm_booking_handler() -> CallbackQueryHandler:
    """Возвращает handler для подтверждения брони."""
    return CallbackQueryHandler(
        confirm_booking_callback,
        pattern=r"^confirm_booking:\d+$"
    )
