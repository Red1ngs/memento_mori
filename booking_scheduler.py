"""Планировщик задач для броней."""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot

from config import TZ, BOOKING_CONFIRM_BEFORE_MINUTES, BOOKING_CONFIRM_GRACE_MINUTES
from database import (
    get_bookings_needing_reminder,
    get_bookings_needing_cancellation,
    get_bookings_to_complete,
    mark_remind_sent,
    cancel_booking,
    complete_booking,
    add_booking_event
)
from notifier import (
    send_booking_reminder,
    send_booking_cancelled_to_user,
    notify_group_booking_cancelled
)
from database import mark_group_notified
from timezone_utils import now_msk, parse_booking_dt, minutes_until, ts_for_db

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ЗАДАЧИ ПЛАНИРОВЩИКА
# ══════════════════════════════════════════════════════════════


async def check_upcoming_bookings(bot: Bot):
    """
    Проверяет брони, которым нужно отправить напоминание за 5 минут.
    
    Условия:
    - status = 'pending'
    - remind_sent = 0
    - до начала осталось <= 5 минут
    """
    try:
        bookings = await get_bookings_needing_reminder()
        
        for booking in bookings:
            # Вычисляем время до начала
            start_dt = parse_booking_dt(booking.date, booking.start_time)
            minutes_left = minutes_until(start_dt)
            
            # Если до начала <= 5 минут, отправляем напоминание
            if 0 <= minutes_left <= BOOKING_CONFIRM_BEFORE_MINUTES:
                success = await send_booking_reminder(bot, booking)
                
                if success:
                    await mark_remind_sent(booking.id)
                    await add_booking_event(
                        booking.id,
                        "remind_sent",
                        "system"
                    )
                    logger.info(f"✅ Напоминание отправлено для брони #{booking.id}")
                    
    except Exception as e:
        logger.error(f"Ошибка в check_upcoming_bookings: {e}", exc_info=True)


async def check_expired_bookings(bot: Bot):
    """
    Проверяет брони, которые нужно отменить по таймауту.
    
    Условия:
    - status = 'pending'
    - remind_sent = 1
    - прошло >= 5 минут после начала
    """
    try:
        bookings = await get_bookings_needing_cancellation()
        
        for booking in bookings:
            # Вычисляем время с начала
            start_dt = parse_booking_dt(booking.date, booking.start_time)
            minutes_since_start = -minutes_until(start_dt)  # отрицательное значение = прошло времени
            
            # Если прошло >= 5 минут после начала, отменяем
            if minutes_since_start >= BOOKING_CONFIRM_GRACE_MINUTES:
                # Отменяем бронь
                await cancel_booking(
                    booking.id,
                    cancelled_by="system",
                    cancel_reason="Не подтверждена в течение 5 минут после начала"
                )
                
                await add_booking_event(
                    booking.id,
                    "cancelled_timeout",
                    "system"
                )
                
                # Уведомляем пользователя
                await send_booking_cancelled_to_user(bot, booking)
                
                # Уведомляем группу
                await notify_group_booking_cancelled(bot, booking, "system")
                await mark_group_notified(booking.id)
                
                logger.info(f"❌ Бронь #{booking.id} отменена по таймауту")
                
    except Exception as e:
        logger.error(f"Ошибка в check_expired_bookings: {e}", exc_info=True)


async def complete_finished_bookings(bot: Bot):
    """
    Завершает подтверждённые брони, время которых истекло.
    
    Условия:
    - status = 'confirmed'
    - текущее время >= end_time
    
    Уведомления не отправляются.
    """
    try:
        bookings = await get_bookings_to_complete()
        
        for booking in bookings:
            # Проверяем, истекло ли время
            end_dt = parse_booking_dt(booking.date, booking.end_time)
            
            if now_msk() >= end_dt:
                # Завершаем бронь
                completed_at = ts_for_db(now_msk())
                await complete_booking(booking.id, completed_at)
                
                await add_booking_event(
                    booking.id,
                    "completed",
                    "system"
                )
                
                logger.info(f"✅ Бронь #{booking.id} завершена")
                
    except Exception as e:
        logger.error(f"Ошибка в complete_finished_bookings: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ ПЛАНИРОВЩИКА
# ══════════════════════════════════════════════════════════════


def init_scheduler(bot: Bot) -> AsyncIOScheduler:
    """
    Инициализирует и запускает планировщик.
    
    Args:
        bot: экземпляр Telegram бота
    
    Returns:
        запущенный планировщик
    """
    scheduler = AsyncIOScheduler(timezone=TZ)
    
    # Проверка напоминаний (каждую минуту)
    scheduler.add_job(
        check_upcoming_bookings,
        'interval',
        minutes=1,
        args=[bot],
        id='check_reminders'
    )
    
    # Проверка истёкших броней (каждую минуту)
    scheduler.add_job(
        check_expired_bookings,
        'interval',
        minutes=1,
        args=[bot],
        id='check_expired'
    )
    
    # Завершение броней (каждую минуту)
    scheduler.add_job(
        complete_finished_bookings,
        'interval',
        minutes=1,
        args=[bot],
        id='complete_bookings'
    )
    
    scheduler.start()
    logger.info("✅ Планировщик броней запущен")
    
    return scheduler
