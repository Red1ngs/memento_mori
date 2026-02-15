"""Пользовательские команды бота."""

import logging
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

from database import (
    get_user,
    delete_user,
    get_user_active_bookings,
    get_user_booking_history,
    get_bookings_for_schedule,
    get_current_card,
    cancel_booking,
    add_booking_event
)
from timezone_utils import get_today_date, get_tomorrow_date, format_date_ru, ts_for_db, now_msk
from schedule_view import format_schedule, format_user_history, format_user_bookings
from notifier import send_booking_cancelled_to_user, notify_group_booking_cancelled
from database import mark_group_notified

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# КОМАНДЫ ПОЛЬЗОВАТЕЛЯ
# ══════════════════════════════════════════════════════════════


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает текущую карту клуба."""
    card = await get_current_card()
    
    if not card:
        await update.message.reply_text("📋 Информация о текущей карте недоступна.")
        return
    
    text = (
        f"🃏 Текущая карта клуба:\n\n"
        f"ID: {card.card_id} | Ранг: {card.card_rank}\n\n"
        f"🔄 Замен: {card.replacements}\n"
        f"📅 Вложено сегодня: {card.daily_donated}\n"
        f"👥 Владельцев в клубе: {len(card.club_owners)}"
    )
    
    if card.card_image_url:
        await update.message.reply_photo(
            photo=card.card_image_url,
            caption=text
        )
    else:
        await update.message.reply_text(text)


async def myaccount_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает информацию о привязанном аккаунте."""
    user = await get_user(update.effective_user.id)
    
    if not user or not user.is_verified:
        await update.message.reply_text(
            "❌ Аккаунт не привязан.\n"
            "Используй /start для регистрации."
        )
        return
    
    status = "✅ Активен" if user.is_active else "⏸ Приостановлен"
    
    text = (
        f"👤 Мой аккаунт:\n\n"
        f"Telegram: {user.tg_nickname}\n"
        f"MangaBuff: {user.mangabuff_nick}\n"
        f"🔗 {user.mangabuff_url}\n\n"
        f"📊 Статус уведомлений: {status}\n"
        f"📅 Зарегистрирован: {user.created_at[:10]}"
    )
    
    await update.message.reply_text(text)


async def unlink_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отвязывает аккаунт MangaBuff."""
    user = await get_user(update.effective_user.id)
    
    if not user:
        await update.message.reply_text("❌ Аккаунт не привязан.")
        return
    
    await delete_user(update.effective_user.id)
    
    await update.message.reply_text(
        "✅ Аккаунт отвязан.\n"
        "Уведомления о картах прекращены.\n\n"
        "Для повторной привязки используй /start"
    )
    
    logger.info(f"Пользователь {user.tg_nickname} отвязал аккаунт")


async def mybookings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает активные брони пользователя."""
    user = await get_user(update.effective_user.id)
    
    if not user or not user.is_verified:
        await update.message.reply_text(
            "❌ Для просмотра броней нужно привязать аккаунт.\n"
            "Используй /start"
        )
        return
    
    today = get_today_date()
    tomorrow = get_tomorrow_date()
    
    bookings = await get_user_active_bookings(user.tg_id, [today, tomorrow])
    text = format_user_bookings(bookings)
    
    await update.message.reply_text(text)


async def cancelbooking_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет активную бронь пользователя."""
    user = await get_user(update.effective_user.id)
    
    if not user or not user.is_verified:
        await update.message.reply_text(
            "❌ Для отмены брони нужно привязать аккаунт.\n"
            "Используй /start"
        )
        return
    
    today = get_today_date()
    tomorrow = get_tomorrow_date()
    
    bookings = await get_user_active_bookings(user.tg_id, [today, tomorrow])
    
    if not bookings:
        await update.message.reply_text("📋 У тебя нет активных броней.")
        return
    
    # Отменяем все активные брони пользователя
    for booking in bookings:
        await cancel_booking(
            booking.id,
            cancelled_by="user",
            cancel_reason="Отменена пользователем",
            actor_tg_id=user.tg_id
        )
        
        await add_booking_event(
            booking.id,
            "cancelled_user",
            "user",
            actor_tg_id=user.tg_id
        )
        
        # Уведомляем пользователя
        bot = context.bot
        await send_booking_cancelled_to_user(bot, booking)
        
        # Уведомляем группу
        await notify_group_booking_cancelled(bot, booking, "user")
        await mark_group_notified(booking.id)
        
        logger.info(f"Пользователь {user.tg_nickname} отменил бронь #{booking.id}")
    
    await update.message.reply_text(
        f"✅ Бронь отменена.\n"
        f"Слот освобождён для других участников."
    )


async def myhistory_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает историю броней пользователя."""
    user = await get_user(update.effective_user.id)
    
    if not user or not user.is_verified:
        await update.message.reply_text(
            "❌ Для просмотра истории нужно привязать аккаунт.\n"
            "Используй /start"
        )
        return
    
    bookings = await get_user_booking_history(user.tg_id, limit=20)
    text = format_user_history(bookings)
    
    await update.message.reply_text(text)


async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает расписание броней на сегодня и завтра."""
    user = await get_user(update.effective_user.id)
    
    if not user or not user.is_verified:
        await update.message.reply_text(
            "❌ Для просмотра расписания нужно привязать аккаунт.\n"
            "Используй /start"
        )
        return
    
    today = get_today_date()
    tomorrow = get_tomorrow_date()
    
    bookings = await get_bookings_for_schedule([today, tomorrow])
    text = format_schedule(bookings, [today, tomorrow])
    
    await update.message.reply_text(text)


# ══════════════════════════════════════════════════════════════
# РЕГИСТРАЦИЯ HANDLERS
# ══════════════════════════════════════════════════════════════


def register_user_handlers(application):
    """Регистрирует пользовательские команды."""
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("myaccount", myaccount_command))
    application.add_handler(CommandHandler("unlink", unlink_command))
    application.add_handler(CommandHandler("mybookings", mybookings_command))
    application.add_handler(CommandHandler("cancelbooking", cancelbooking_command))
    application.add_handler(CommandHandler("myhistory", myhistory_command))
    application.add_handler(CommandHandler("schedule", schedule_command))
    
    logger.info("✅ Пользовательские команды зарегистрированы")