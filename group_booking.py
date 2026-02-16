"""Бронирование через inline-кнопки в группе."""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CallbackQueryHandler

from database import (
    create_booking,
    get_bookings_for_schedule,
    BookingConflictError
)
from timezone_utils import (
    get_today_date,
    get_tomorrow_date,
    format_date_ru,
    format_duration,
    calculate_duration_hours,
)
from booking_validator import (
    get_available_start_slots,
    get_available_end_slots,
    validate_booking_slot,
    MAX_INLINE_SLOTS
)
from booking_utils import (
    get_verified_user,
    get_active_bookings_today_tomorrow,
    format_active_bookings_text
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ГЛАВНОЕ МЕНЮ БРОНИРОВАНИЯ
# ══════════════════════════════════════════════════════════════


async def show_booking_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню выбора даты для бронирования."""
    user = update.effective_user

    db_user = await get_verified_user(user.id)
    if not db_user:
        await update.message.reply_text(
            "❌ Для бронирования нужно привязать аккаунт.\n"
            "Напиши мне в личные сообщения: /start"
        )
        return

    existing = await get_active_bookings_today_tomorrow(user.id)
    if existing:
        await update.message.reply_text(
            format_active_bookings_text(existing, for_group=True)
        )
        return

    await _send_date_menu(update.message.reply_text)


async def _send_date_menu(reply_fn):
    """Вспомогательная функция: отправляет/редактирует меню выбора даты."""
    today = get_today_date()
    tomorrow = get_tomorrow_date()

    keyboard = [
        [InlineKeyboardButton(
            f"📅 Сегодня, {format_date_ru(today)}",
            callback_data=f"book_date:{today}"
        )],
        [InlineKeyboardButton(
            f"📅 Завтра, {format_date_ru(tomorrow)}",
            callback_data=f"book_date:{tomorrow}"
        )]
    ]

    await reply_fn(
        "📅 Выбери дату для бронирования:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ══════════════════════════════════════════════════════════════
# ВЫБОР ДАТЫ
# ══════════════════════════════════════════════════════════════


async def handle_date_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора даты."""
    query = update.callback_query
    await query.answer()

    date = query.data.split(":")[1]

    busy_bookings = await get_bookings_for_schedule([date])
    available_slots = get_available_start_slots(date, busy_bookings)

    if not available_slots:
        await query.edit_message_text(
            f"😔 На {format_date_ru(date)} все слоты заняты.\n"
            f"Попробуй выбрать другую дату."
        )
        return

    keyboard = _build_slots_keyboard(
        available_slots[:MAX_INLINE_SLOTS],
        callback_prefix=f"book_start:{date}",
        back_callback="book_menu"
    )

    await query.edit_message_text(
        f"🕐 Дата: {format_date_ru(date)}\n\n"
        f"Выбери время начала брони:\n"
        f"(максимальная длительность — 2 часа)",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ══════════════════════════════════════════════════════════════
# ВЫБОР ВРЕМЕНИ НАЧАЛА
# ══════════════════════════════════════════════════════════════


async def handle_start_time_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора времени начала."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")
    date = parts[1]
    # ИСПРАВЛЕНО: Время формата HH:MM разбивается на parts[2] и parts[3]
    # callback_data: "book_start:2026-02-16:21:00" -> ["book_start", "2026-02-16", "21", "00"]
    start_time = f"{parts[2]}:{parts[3]}"

    busy_bookings = await get_bookings_for_schedule([date])
    available_slots = get_available_end_slots(date, start_time, busy_bookings)

    if not available_slots:
        await query.edit_message_text(
            "😔 Нет доступных слотов окончания для этого времени.\n"
            "Попробуй другое время начала."
        )
        return

    keyboard = _build_slots_keyboard(
        available_slots,
        callback_prefix=f"book_end:{date}:{start_time}",
        back_callback=f"book_date:{date}"
    )

    await query.edit_message_text(
        f"🕐 Дата: {format_date_ru(date)}\n"
        f"⏰ Начало: {start_time}\n\n"
        f"Выбери время окончания:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ══════════════════════════════════════════════════════════════
# СОЗДАНИЕ БРОНИ
# ══════════════════════════════════════════════════════════════


async def handle_end_time_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора времени окончания и создание брони."""
    query = update.callback_query
    await query.answer("⏳ Создаю бронь...")

    user = query.from_user
    parts = query.data.split(":")
    date = parts[1]
    # ИСПРАВЛЕНО: Правильный парсинг времени
    # callback_data: "book_end:2026-02-16:21:00:22:00" -> ["book_end", "2026-02-16", "21", "00", "22", "00"]
    start_time = f"{parts[2]}:{parts[3]}"
    end_time = f"{parts[4]}:{parts[5]}"

    db_user = await get_verified_user(user.id)
    if not db_user:
        await query.edit_message_text(
            "❌ Для бронирования нужно привязать аккаунт.\n"
            "Напиши мне в личные сообщения: /start"
        )
        return

    # Финальная валидация (race condition guard)
    is_valid, error_msg = await validate_booking_slot(date, start_time, end_time)
    if not is_valid:
        await query.edit_message_text(
            f"⚠️ {error_msg}\n"
            f"Кто-то успел забронировать этот слот быстрее."
        )
        return

    duration_hours = calculate_duration_hours(start_time, end_time)

    try:
        booking_id = await create_booking(
            tg_id=db_user.tg_id,
            tg_nickname=db_user.tg_nickname,
            mangabuff_nick=db_user.mangabuff_nick,
            date=date,
            start_time=start_time,
            end_time=end_time,
            duration_hours=duration_hours
        )
    except BookingConflictError:
        await query.edit_message_text(
            "⚠️ У тебя уже есть бронь на этот день.\n"
            "Одна дата — одна бронь."
        )
        return

    await query.edit_message_text(
        f"✅ Бронь успешно создана!\n\n"
        f"🃏 Назначение: внос карт в клуб Таро\n"
        f"📅 Дата: {format_date_ru(date)}\n"
        f"🕐 Время: {start_time} — {end_time} МСК\n"
        f"⏱ Длительность: {format_duration(duration_hours)}\n"
        f"👤 {db_user.tg_nickname} / {db_user.mangabuff_nick}\n\n"
        f"⚠️ За 5 минут до начала придёт уведомление.\n"
        f"Не подтвердишь в течение 5 минут после начала — бронь отменится."
    )

    logger.info(
        f"✅ Создана бронь #{booking_id} из группы: {db_user.tg_nickname} "
        f"на {date} {start_time}-{end_time}"
    )


# ══════════════════════════════════════════════════════════════
# ВОЗВРАТ В ГЛАВНОЕ МЕНЮ
# ══════════════════════════════════════════════════════════════


async def handle_back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню бронирования."""
    query = update.callback_query
    await query.answer()

    user = query.from_user

    db_user = await get_verified_user(user.id)
    if not db_user:
        await query.edit_message_text(
            "❌ Для бронирования нужно привязать аккаунт.\n"
            "Напиши мне в личные сообщения: /start"
        )
        return

    existing = await get_active_bookings_today_tomorrow(user.id)
    if existing:
        await query.edit_message_text(
            format_active_bookings_text(existing, for_group=True)
        )
        return

    await _send_date_menu(query.edit_message_text)


# ══════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════


def _build_slots_keyboard(
    slots: list,
    callback_prefix: str,
    back_callback: str,
    per_row: int = 4
) -> list:
    """
    Строит inline-клавиатуру из слотов времени.

    Args:
        slots: список слотов ["14:30", "15:00", ...]
        callback_prefix: префикс для callback_data (слот добавляется через :)
        back_callback: callback_data кнопки «Назад»
        per_row: кнопок в ряду

    Returns:
        список рядов InlineKeyboardButton
    """
    keyboard = []
    row = []
    for slot in slots:
        row.append(InlineKeyboardButton(
            slot,
            callback_data=f"{callback_prefix}:{slot}"
        ))
        if len(row) == per_row:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=back_callback)])
    return keyboard


# ══════════════════════════════════════════════════════════════
# РЕГИСТРАЦИЯ HANDLERS
# ══════════════════════════════════════════════════════════════


def register_group_booking_handlers(application):
    """Регистрирует handlers для бронирования в группах."""
    application.add_handler(
        CallbackQueryHandler(handle_date_selection, pattern=r"^book_date:")
    )
    application.add_handler(
        CallbackQueryHandler(handle_start_time_selection, pattern=r"^book_start:")
    )
    application.add_handler(
        CallbackQueryHandler(handle_end_time_selection, pattern=r"^book_end:")
    )
    application.add_handler(
        CallbackQueryHandler(handle_back_to_menu, pattern=r"^book_menu$")
    )

    logger.info("✅ Handlers для группового бронирования зарегистрированы")