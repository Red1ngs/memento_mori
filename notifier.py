"""Модуль уведомлений."""

import logging
from typing import Dict, Any
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError

from config import BASE_URL, CLUB_BOOST_PATH, REQUIRED_TG_GROUP_ID
from database import get_user_by_mangabuff_id, Booking
from timezone_utils import format_date_ru, format_time_range

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# УВЕДОМЛЕНИЯ О КАРТАХ
# ══════════════════════════════════════════════════════════════


async def notify_owners(bot: Bot, card_data: Dict[str, Any]):
    """
    Отправляет уведомления владельцам карты.
    
    Args:
        bot: экземпляр Telegram бота
        card_data: данные карты из парсера
    """
    owner_ids = card_data.get("club_owners", [])
    
    if not owner_ids:
        logger.info("Нет владельцев карты для уведомления")
        return
    
    logger.info(f"Отправка уведомлений {len(owner_ids)} владельцам карты")
    
    sent_count = 0
    for mangabuff_id in owner_ids:
        if await send_card_notification(bot, mangabuff_id, card_data):
            sent_count += 1
    
    logger.info(f"✅ Отправлено {sent_count}/{len(owner_ids)} уведомлений")


async def send_card_notification(
    bot: Bot,
    mangabuff_id: int,
    card_data: Dict[str, Any]
) -> bool:
    """
    Отправляет уведомление одному пользователю.
    
    Args:
        bot: экземпляр Telegram бота
        mangabuff_id: ID пользователя на MangaBuff
        card_data: данные карты
    
    Returns:
        True если успешно отправлено
    """
    try:
        # Получаем пользователя из БД
        user = await get_user_by_mangabuff_id(mangabuff_id)
        
        if not user:
            logger.debug(f"Пользователь {mangabuff_id} не найден в БД")
            return False
        
        if not user.is_active:
            logger.debug(f"Пользователь {user.tg_nickname} отключил уведомления")
            return False
        
        if not user.is_verified:
            logger.debug(f"Пользователь {user.tg_nickname} не верифицирован")
            return False
        
        # Формируем сообщение (ИСПРАВЛЕНО: без card_name)
        text = (
            f"🔴 У вас есть нужная карта клуба!\n\n"
            f"ID: {card_data['card_id']} | Ранг: {card_data['card_rank']}\n\n"
            f"🎯 Аккаунт: {user.mangabuff_nick}\n"
            f"🔄 Замен: {card_data['replacements']}\n"
            f"📅 Вложено сегодня: {card_data['daily_donated']}"
        )
        
        # Кнопка для перехода на страницу boost
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "🚀 Внести карту в клуб",
                url=f"{BASE_URL}{CLUB_BOOST_PATH}"
            )
        ]])
        
        # Отправляем с изображением, если есть
        if card_data.get("card_image_url"):
            await bot.send_photo(
                chat_id=user.tg_id,
                photo=card_data["card_image_url"],
                caption=text,
                reply_markup=keyboard
            )
        else:
            await bot.send_message(
                chat_id=user.tg_id,
                text=text,
                reply_markup=keyboard
            )
        
        logger.info(f"✅ Уведомление отправлено: {user.tg_nickname}")
        return True
        
    except TelegramError as e:
        logger.error(f"Ошибка отправки уведомления пользователю {mangabuff_id}: {e}")
        return False


# ══════════════════════════════════════════════════════════════
# УВЕДОМЛЕНИЯ О БРОНЯХ
# ══════════════════════════════════════════════════════════════


async def send_booking_reminder(bot: Bot, booking: Booking) -> bool:
    """
    Отправляет напоминание о брони за 5 минут.
    
    Args:
        bot: экземпляр Telegram бота
        booking: бронь
    
    Returns:
        True если успешно
    """
    try:
        text = (
            f"⏰ Твоя бронь начинается через 5 минут!\n\n"
            f"🃏 Внос карт в клуб\n"
            f"📅 {format_date_ru(booking.date)} | "
            f"🕐 {format_time_range(booking.start_time, booking.end_time)}\n\n"
            f"Подтверди участие — иначе бронь будет отменена\n"
            f"через 5 минут после начала."
        )
        
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "✅ Подтвердить бронь",
                callback_data=f"confirm_booking:{booking.id}"
            )
        ]])
        
        await bot.send_message(
            chat_id=booking.tg_id,
            text=text,
            reply_markup=keyboard
        )
        
        logger.info(f"✅ Напоминание отправлено: бронь #{booking.id}")
        return True
        
    except TelegramError as e:
        logger.error(f"Ошибка отправки напоминания для брони #{booking.id}: {e}")
        return False


async def send_booking_cancelled_to_user(bot: Bot, booking: Booking) -> bool:
    """
    Отправляет уведомление об отмене брони пользователю.
    
    Args:
        bot: экземпляр Telegram бота
        booking: отменённая бронь
    
    Returns:
        True если успешно
    """
    try:
        reason_text = {
            "system": "Ты не подтвердил бронь вовремя.",
            "user": "Ты отменил бронь.",
            "admin": "Бронь была отменена администратором."
        }.get(booking.cancelled_by, "Бронь отменена.")
        
        text = (
            f"❌ Бронь отменена\n\n"
            f"{reason_text}\n\n"
            f"📅 {format_date_ru(booking.date)} | "
            f"🕐 {format_time_range(booking.start_time, booking.end_time)}\n\n"
            f"Слот освобождён. Можешь создать новую бронь — напиши «забронировать»."
        )
        
        await bot.send_message(
            chat_id=booking.tg_id,
            text=text
        )
        
        logger.info(f"✅ Уведомление об отмене отправлено: бронь #{booking.id}")
        return True
        
    except TelegramError as e:
        logger.error(f"Ошибка отправки уведомления об отмене брони #{booking.id}: {e}")
        return False


async def notify_group_booking_cancelled(
    bot: Bot,
    booking: Booking,
    cancelled_by: str
) -> bool:
    """
    Отправляет уведомление в группу об отмене брони.
    
    Args:
        bot: экземпляр Telegram бота
        booking: отменённая бронь
        cancelled_by: кто отменил ('system', 'user', 'admin')
    
    Returns:
        True если успешно
    """
    try:
        emoji_map = {
            "system": "❌",
            "user": "🚫",
            "admin": "🚫"
        }
        
        reason_map = {
            "system": f"{booking.mangabuff_nick} не подтвердил бронь вовремя.",
            "user": f"{booking.mangabuff_nick} отменил свою бронь.",
            "admin": f"Бронь {booking.mangabuff_nick} была отменена."
        }
        
        emoji = emoji_map.get(cancelled_by, "❌")
        reason = reason_map.get(cancelled_by, "Бронь отменена.")
        
        title = "🔔 Бронь отменена"
        if cancelled_by == "admin":
            title = "🔔 Бронь отменена администратором"
        
        text = (
            f"{title}\n\n"
            f"{emoji} {reason}\n\n"
            f"📅 {format_date_ru(booking.date)} | "
            f"🕐 {format_time_range(booking.start_time, booking.end_time)}\n\n"
            f"🆓 Время освободилось — пиши «забронировать»!"
        )
        
        await bot.send_message(
            chat_id=REQUIRED_TG_GROUP_ID,
            text=text
        )
        
        logger.info(f"✅ Группа уведомлена об отмене брони #{booking.id}")
        return True
        
    except TelegramError as e:
        logger.error(f"Ошибка уведомления группы об отмене брони #{booking.id}: {e}")
        return False