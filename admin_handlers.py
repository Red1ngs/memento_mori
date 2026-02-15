"""Команды администратора."""

import logging
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

from config import ADMIN_TG_ID
from database import (
    get_all_users,
    delete_user,
    toggle_user_active,
    get_user,
    get_all_booking_history,
    get_user_booking_history,
    get_booking,
    cancel_booking,
    add_booking_event
)
from schedule_view import format_all_history, format_user_history
from notifier import send_booking_cancelled_to_user, notify_group_booking_cancelled
from database import mark_group_notified

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ДЕКОРАТОР ПРОВЕРКИ ПРАВ
# ══════════════════════════════════════════════════════════════


def admin_only(func):
    """Декоратор для проверки прав администратора."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_TG_ID:
            await update.message.reply_text("❌ Эта команда доступна только администратору.")
            return
        return await func(update, context)
    return wrapper


# ══════════════════════════════════════════════════════════════
# КОМАНДЫ АДМИНИСТРАТОРА
# ══════════════════════════════════════════════════════════════


@admin_only
async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список всех пользователей бота."""
    users = await get_all_users()
    
    if not users:
        await update.message.reply_text("📋 Пользователей нет.")
        return
    
    text = f"👥 Пользователи бота ({len(users)}):\n\n"
    
    for user in users:
        status = "✅" if user.is_active else "⏸"
        verified = "✓" if user.is_verified else "✗"
        
        text += (
            f"{status} {user.tg_nickname} (@{user.tg_username or 'нет'})\n"
            f"   TG ID: {user.tg_id}\n"
            f"   MB: {user.mangabuff_nick} (ID: {user.mangabuff_id})\n"
            f"   Верифицирован: {verified}\n\n"
        )
    
    # Разбиваем на части, если слишком длинное
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text(text)


@admin_only
async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет пользователя. Использование: /removeuser <tg_id>"""
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ Использование: /removeuser <tg_id>\n"
            "Пример: /removeuser 123456789"
        )
        return
    
    try:
        tg_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Неверный формат TG ID.")
        return
    
    user = await get_user(tg_id)
    if not user:
        await update.message.reply_text(f"❌ Пользователь с TG ID {tg_id} не найден.")
        return
    
    await delete_user(tg_id)
    
    await update.message.reply_text(
        f"✅ Пользователь удалён:\n"
        f"TG: {user.tg_nickname} ({tg_id})\n"
        f"MB: {user.mangabuff_nick}"
    )
    
    logger.info(f"Администратор удалил пользователя {user.tg_nickname} (TG: {tg_id})")


@admin_only
async def toggleuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключает уведомления пользователя. Использование: /toggleuser <tg_id>"""
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ Использование: /toggleuser <tg_id>\n"
            "Пример: /toggleuser 123456789"
        )
        return
    
    try:
        tg_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Неверный формат TG ID.")
        return
    
    user = await get_user(tg_id)
    if not user:
        await update.message.reply_text(f"❌ Пользователь с TG ID {tg_id} не найден.")
        return
    
    new_status = await toggle_user_active(tg_id)
    status_text = "включены" if new_status else "выключены"
    
    await update.message.reply_text(
        f"✅ Уведомления {status_text} для:\n"
        f"TG: {user.tg_nickname} ({tg_id})\n"
        f"MB: {user.mangabuff_nick}"
    )
    
    logger.info(f"Администратор изменил статус уведомлений для {user.tg_nickname}: {status_text}")


@admin_only
async def syncclub_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительный переспарс списка членов клуба."""
    await update.message.reply_text(
        "⏳ Синхронизация списка членов клуба...\n"
        "(Эта функция требует реализации парсера страницы клуба)"
    )
    
    # TODO: Реализовать полный парсинг страницы клуба
    # и обновление информации о пользователях
    
    logger.info("Администратор запустил синхронизацию клуба")


@admin_only
async def allbookings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает все активные брони."""
    from database import get_bookings_for_schedule
    from timezone_utils import get_today_date, get_tomorrow_date
    from schedule_view import format_schedule
    
    today = get_today_date()
    tomorrow = get_tomorrow_date()
    
    bookings = await get_bookings_for_schedule([today, tomorrow])
    text = format_schedule(bookings, [today, tomorrow])
    
    await update.message.reply_text(text)


@admin_only
async def bookinghistory_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает историю броней.
    Использование:
    - /bookinghistory <tg_id> - история конкретного пользователя
    - /bookinghistory all - полная история всех броней
    """
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ Использование:\n"
            "/bookinghistory <tg_id> - история пользователя\n"
            "/bookinghistory all - полная история"
        )
        return
    
    arg = context.args[0]
    
    if arg.lower() == "all":
        # Полная история
        bookings = await get_all_booking_history(limit=50)
        text = format_all_history(bookings)
    else:
        # История конкретного пользователя
        try:
            tg_id = int(arg)
        except ValueError:
            await update.message.reply_text("❌ Неверный формат TG ID.")
            return
        
        user = await get_user(tg_id)
        if not user:
            await update.message.reply_text(f"❌ Пользователь с TG ID {tg_id} не найден.")
            return
        
        bookings = await get_user_booking_history(tg_id, limit=20)
        text = f"📜 История броней: {user.tg_nickname}\n\n"
        text += format_user_history(bookings)
    
    # Разбиваем на части, если слишком длинное
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text(text)


@admin_only
async def admincancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Принудительная отмена брони.
    Использование: /admincancel <booking_id>
    """
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ Использование: /admincancel <booking_id>\n"
            "Пример: /admincancel 123"
        )
        return
    
    try:
        booking_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID брони.")
        return
    
    booking = await get_booking(booking_id)
    if not booking:
        await update.message.reply_text(f"❌ Бронь #{booking_id} не найдена.")
        return
    
    if booking.status not in ["pending", "confirmed"]:
        await update.message.reply_text(
            f"❌ Бронь #{booking_id} уже неактивна (статус: {booking.status})."
        )
        return
    
    # Отменяем бронь
    await cancel_booking(
        booking_id,
        cancelled_by="admin",
        cancel_reason="Отменена администратором",
        actor_tg_id=update.effective_user.id
    )
    
    await add_booking_event(
        booking_id,
        "cancelled_admin",
        "admin",
        actor_tg_id=update.effective_user.id
    )
    
    # Уведомляем пользователя
    bot = context.bot
    await send_booking_cancelled_to_user(bot, booking)
    
    # Уведомляем группу
    await notify_group_booking_cancelled(bot, booking, "admin")
    await mark_group_notified(booking_id)
    
    await update.message.reply_text(
        f"✅ Бронь #{booking_id} отменена.\n"
        f"Пользователь: {booking.tg_nickname}\n"
        f"Дата: {booking.date} {booking.start_time}-{booking.end_time}"
    )
    
    logger.info(
        f"Администратор отменил бронь #{booking_id} "
        f"пользователя {booking.tg_nickname}"
    )


# ══════════════════════════════════════════════════════════════
# РЕГИСТРАЦИЯ HANDLERS
# ══════════════════════════════════════════════════════════════


def register_admin_handlers(application):
    """Регистрирует команды администратора."""
    application.add_handler(CommandHandler("listusers", listusers_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("toggleuser", toggleuser_command))
    application.add_handler(CommandHandler("syncclub", syncclub_command))
    application.add_handler(CommandHandler("allbookings", allbookings_command))
    application.add_handler(CommandHandler("bookinghistory", bookinghistory_command))
    application.add_handler(CommandHandler("admincancel", admincancel_command))
    
    logger.info("✅ Команды администратора зарегистрированы")
