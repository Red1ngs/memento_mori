"""Модуль уведомлений."""

import logging
from typing import Dict, Any, List, Tuple
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError

from config import BASE_URL, CLUB_BOOST_PATH, REQUIRED_TG_GROUP_ID, GROUP_CARD_TOPIC_ID, GROUP_ALLIANCE_TOPIC_ID
from database import get_user_by_mangabuff_id, Booking
from timezone_utils import format_date_ru, format_time_range, now_msk

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# УВЕДОМЛЕНИЯ О КАРТАХ — ЛИЧНЫЕ СООБЩЕНИЯ ВЛАДЕЛЬЦАМ
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

        text = (
            f"🔴 У вас есть нужная карта клуба!\n\n"
            f"ID: {card_data['card_id']} | Ранг: {card_data['card_rank']}\n\n"
            f"🎯 Аккаунт: {user.mangabuff_nick}\n"
            f"🔄 Замен: {card_data['replacements']}\n"
            f"📅 Вложено сегодня: {card_data['daily_donated']}"
        )

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "🚀 Внести карту в клуб",
                url=f"{BASE_URL}{CLUB_BOOST_PATH}"
            )
        ]])

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
# УВЕДОМЛЕНИЕ О НОВОЙ КАРТЕ В ТОПИК ГРУППЫ
# ══════════════════════════════════════════════════════════════


async def notify_group_new_card(
    bot: Bot,
    card_data: Dict[str, Any],
    card_name: str,
    owners_nicks: List[Tuple[int, str]]
) -> bool:
    """
    Отправляет уведомление о новой карте клуба в топик группы.

    Формат сообщения:
    1. Картинка
    2. Имя карты
    3. Ранг
    4. Какая по счёту карта вложена сегодня
    5. Кто из участников клуба имеет эту карту
    6. Ссылка на страницу вклада
    7. Время вклада

    Args:
        bot: экземпляр Telegram бота
        card_data: данные карты (card_id, card_rank, card_image_url,
                   replacements, daily_donated, club_owners, discovered_at)
        card_name: название карты (получено с /cards/{id}/users)
        owners_nicks: список (user_id, nickname) владельцев карты в клубе

    Returns:
        True если успешно отправлено
    """
    try:
        card_id = card_data.get("card_id", "?")
        card_rank = card_data.get("card_rank", "?")
        replacements = card_data.get("replacements", "?")
        daily_donated = card_data.get("daily_donated", "?")
        card_image_url = card_data.get("card_image_url", "")

        # Парсим счётчик "какая по счёту" из daily_donated (формат "X/Y")
        donated_count = _parse_first_number(daily_donated)
        donated_ordinal = _make_ordinal(donated_count) if donated_count else daily_donated

        # Время вклада (МСК)
        now = now_msk()
        time_str = now.strftime("%H:%M МСК")
        date_str = now.strftime("%d.%m.%Y")

        # Блок с владельцами
        if owners_nicks:
            owners_lines = "\n".join(
                f"  • <a href=\"{BASE_URL}/users/{uid}\">{nick}</a>"
                for uid, nick in owners_nicks
            )
            owners_block = f"👥 <b>Есть у участников клуба:</b>\n{owners_lines}"
        else:
            owners_block = "👥 <b>Владельцев в клубе нет</b>"

        # Ссылка на страницу вклада
        boost_url = f"{BASE_URL}{CLUB_BOOST_PATH}"
        card_url = f"{BASE_URL}/cards/{card_id}/users"

        text = (
            f"🃏 <b>{card_name}</b>\n"
            f"⭐ Ранг: <b>{card_rank}</b>\n\n"
            f"📊Вкладов сегодня: {daily_donated}\n"
            f"{owners_block}\n\n"
            f"🔗 <a href=\"{boost_url}\">Внести карту в клуб</a>\n"
            f"⏰ {date_str} {time_str}"
        )

        # Отправляем в топик группы
        send_kwargs = {
            "chat_id": REQUIRED_TG_GROUP_ID,
            "parse_mode": "HTML",
            "message_thread_id": GROUP_CARD_TOPIC_ID,
        }

        if card_image_url:
            await bot.send_photo(
                photo=card_image_url,
                caption=text,
                **send_kwargs
            )
        else:
            await bot.send_message(
                text=text,
                **send_kwargs
            )

        logger.info(
            f"✅ Уведомление о карте #{card_id} «{card_name}» "
            f"отправлено в топик {GROUP_CARD_TOPIC_ID}"
        )
        return True

    except TelegramError as e:
        logger.error(f"Ошибка отправки уведомления о карте в группу: {e}")
        return False
    except Exception as e:
        logger.error(f"Непредвиденная ошибка в notify_group_new_card: {e}", exc_info=True)
        return False


def _parse_first_number(value: str) -> int:
    """Извлекает первое число из строки формата 'X/Y'."""
    try:
        return int(str(value).split("/")[0].strip())
    except (ValueError, IndexError):
        return 0


def _make_ordinal(n: int) -> str:
    """Возвращает порядковое числительное на русском: 1-я, 2-я, 3-я..."""
    if n <= 0:
        return "?"
    # Исключения для 11, 12, 13, 14
    if 11 <= (n % 100) <= 14:
        return f"{n}-я"
    last = n % 10
    if last == 1:
        return f"{n}-я"
    elif last in (2, 3, 4):
        return f"{n}-я"
    else:
        return f"{n}-я"


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

    Пропускает отправку если группа уже была уведомлена (group_notified=1),
    чтобы исключить дублирование при сбоях планировщика.

    Args:
        bot: экземпляр Telegram бота
        booking: отменённая бронь
        cancelled_by: кто отменил ('system', 'user', 'admin')

    Returns:
        True если успешно
    """
    # Идемпотентность: не слать повторно если группа уже уведомлена
    if booking.group_notified:
        logger.debug(f"Группа уже уведомлена о брони #{booking.id}, пропускаем")
        return True

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

        title = "🔔 Бронь отменена администратором" if cancelled_by == "admin" else "🔔 Бронь отменена"

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


# ══════════════════════════════════════════════════════════════
# УВЕДОМЛЕНИЯ ОБ АЛЬЯНСЕ
# ══════════════════════════════════════════════════════════════


async def notify_alliance_manga_changed(
    bot: Bot,
    manga_info: dict,
    is_startup: bool = False
) -> bool:
    """
    Отправляет уведомление о смене тайтла в альянсе в топик группы.

    Args:
        bot: экземпляр Telegram бота
        manga_info: данные манги (slug, title, image, url, discovered_at)
        is_startup: True если это первый запуск (не смена, а инициализация)

    Returns:
        True если успешно
    """
    from datetime import datetime as dt

    title = manga_info.get("title", manga_info.get("slug", "???"))
    image = manga_info.get("image")
    url = manga_info.get("url", "")

    now_str = dt.now().strftime("%d.%m.%Y %H:%M:%S")

    if is_startup:
        header = "🚀 <b>Мониторинг альянса запущен</b>"
    else:
        header = "🔔 <b>Смена тайтла в альянсе!</b>"

    text = (
        f"{header}\n\n"
        f"📚 <code>{title}</code>\n\n"
        f"🔗 <a href=\"{BASE_URL + '/alliances/45/boost'}\">Перейти к альянсу</a>\n\n"
        f"⏰ {now_str}"
    )

    send_kwargs = {
        "chat_id": REQUIRED_TG_GROUP_ID,
        "parse_mode": "HTML",
        "message_thread_id": GROUP_ALLIANCE_TOPIC_ID,
    }

    try:
        if image:
            await bot.send_photo(
                photo=image,
                caption=text,
                **send_kwargs
            )
        else:
            await bot.send_message(
                text=text,
                **send_kwargs
            )

        logger.info(
            f"✅ Уведомление альянса отправлено в топик {GROUP_ALLIANCE_TOPIC_ID}: "
            f"{title} ({'старт' if is_startup else 'смена'})"
        )
        return True

    except TelegramError as e:
        logger.error(f"Ошибка отправки уведомления альянса: {e}")
        return False