"""FSM бронирования (личные сообщения)."""

import logging
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)

from database import (
    create_booking,
    get_bookings_for_schedule,
    add_booking_event,
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
    format_time_slots_keyboard,
    validate_booking_slot
)
from booking_utils import (
    get_verified_user,
    get_active_bookings_today_tomorrow,
    format_active_bookings_text
)

logger = logging.getLogger(__name__)

# Состояния FSM
STEP_DATE = 1
STEP_START_TIME = 2
STEP_END_TIME = 3


# ══════════════════════════════════════════════════════════════
# HANDLERS
# ══════════════════════════════════════════════════════════════


async def start_booking_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса бронирования."""
    user = update.effective_user

    db_user = await get_verified_user(user.id)
    if not db_user:
        await update.message.reply_text(
            "❌ Для бронирования нужно привязать аккаунт.\n"
            "Используй /start"
        )
        return ConversationHandler.END

    # Проверка существующих броней
    existing = await get_active_bookings_today_tomorrow(user.id)
    if existing:
        await update.message.reply_text(
            format_active_bookings_text(existing, for_group=False)
        )
        return ConversationHandler.END

    context.user_data["booking_user"] = db_user

    today = get_today_date()
    tomorrow = get_tomorrow_date()

    keyboard = [
        [f"📅 Сегодня, {format_date_ru(today)}", f"📅 Завтра, {format_date_ru(tomorrow)}"],
        ["❌ Отмена"]
    ]

    await update.message.reply_text(
        "📅 Выбери дату бронирования:",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    )

    return STEP_DATE


async def receive_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора даты."""
    text = update.message.text

    if text == "❌ Отмена":
        return await cancel_booking_flow(update, context)

    today = get_today_date()
    tomorrow = get_tomorrow_date()

    if "Сегодня" in text:
        selected_date = today
    elif "Завтра" in text:
        selected_date = tomorrow
    else:
        await update.message.reply_text(
            "❌ Неверный выбор. Используй кнопки.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    context.user_data["booking_date"] = selected_date

    busy_bookings = await get_bookings_for_schedule([selected_date])
    available_slots = get_available_start_slots(selected_date, busy_bookings)

    if not available_slots:
        await update.message.reply_text(
            f"😔 На {format_date_ru(selected_date)} все слоты заняты.\n"
            f"Попробуй выбрать другую дату.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    keyboard = format_time_slots_keyboard(available_slots, per_row=4)
    keyboard.append(["◀️ Назад", "❌ Отмена"])

    await update.message.reply_text(
        f"🕐 Выбери время начала брони:\n"
        f"(максимальная длительность — 2 часа)",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    )

    return STEP_START_TIME


async def receive_start_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора времени начала."""
    text = update.message.text

    if text == "❌ Отмена":
        return await cancel_booking_flow(update, context)

    if text == "◀️ Назад":
        return await start_booking_flow(update, context)

    start_time = text.strip()

    if not start_time or ":" not in start_time:
        await update.message.reply_text(
            "❌ Неверный формат времени. Используй кнопки.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    context.user_data["booking_start_time"] = start_time

    selected_date = context.user_data["booking_date"]
    busy_bookings = await get_bookings_for_schedule([selected_date])
    available_slots = get_available_end_slots(selected_date, start_time, busy_bookings)

    if not available_slots:
        await update.message.reply_text(
            "😔 Нет доступных слотов окончания для этого времени.\n"
            "Попробуй другое время начала.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    keyboard = format_time_slots_keyboard(available_slots, per_row=4)
    keyboard.append(["◀️ Назад", "❌ Отмена"])

    await update.message.reply_text(
        f"🕐 Начало: {start_time}\n"
        f"Выбери время окончания:",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    )

    return STEP_END_TIME


async def receive_end_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора времени окончания и создание брони."""
    text = update.message.text

    if text == "❌ Отмена":
        return await cancel_booking_flow(update, context)

    if text == "◀️ Назад":
        context.user_data.pop("booking_start_time", None)

        selected_date = context.user_data["booking_date"]
        busy_bookings = await get_bookings_for_schedule([selected_date])
        available_slots = get_available_start_slots(selected_date, busy_bookings)

        keyboard = format_time_slots_keyboard(available_slots, per_row=4)
        keyboard.append(["◀️ Назад", "❌ Отмена"])

        await update.message.reply_text(
            f"🕐 Выбери время начала брони:\n"
            f"(максимальная длительность — 2 часа)",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        )

        return STEP_START_TIME

    end_time = text.strip()

    if not end_time or ":" not in end_time:
        await update.message.reply_text(
            "❌ Неверный формат времени. Используй кнопки.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    db_user = context.user_data["booking_user"]
    date = context.user_data["booking_date"]
    start_time = context.user_data["booking_start_time"]

    # Финальная валидация (race condition guard)
    is_valid, error_msg = await validate_booking_slot(date, start_time, end_time)
    if not is_valid:
        await update.message.reply_text(
            f"⚠️ {error_msg}\n"
            f"Пожалуйста, выбери другое время.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

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
        await update.message.reply_text(
            "⚠️ У тебя уже есть бронь на этот день.\n"
            "Одна дата — одна бронь.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    await update.message.reply_text(
        f"✅ Бронь успешно создана!\n\n"
        f"🃏 Назначение: внос карт в клуб Таро\n"
        f"📅 Дата: {format_date_ru(date)}\n"
        f"🕐 Время: {start_time} — {end_time} МСК\n"
        f"⏱ Длительность: {format_duration(duration_hours)}\n"
        f"👤 {db_user.tg_nickname} / {db_user.mangabuff_nick}\n\n"
        f"⚠️ За 5 минут до начала придёт уведомление.\n"
        f"Не подтвердишь в течение 5 минут после начала — бронь отменится.",
        reply_markup=ReplyKeyboardRemove()
    )

    logger.info(
        f"✅ Создана бронь #{booking_id}: {db_user.tg_nickname} "
        f"на {date} {start_time}-{end_time}"
    )

    context.user_data.clear()
    return ConversationHandler.END


async def cancel_booking_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена процесса бронирования."""
    await update.message.reply_text(
        "❌ Бронирование отменено.",
        reply_markup=ReplyKeyboardRemove()
    )
    context.user_data.clear()
    return ConversationHandler.END


# ══════════════════════════════════════════════════════════════
# CONVERSATION HANDLER
# ══════════════════════════════════════════════════════════════


def get_booking_conversation_handler() -> ConversationHandler:
    """Возвращает ConversationHandler для бронирования."""
    return ConversationHandler(
        entry_points=[],
        states={
            STEP_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_date)
            ],
            STEP_START_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_start_time)
            ],
            STEP_END_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_end_time)
            ]
        },
        fallbacks=[
            MessageHandler(filters.Regex(r"^(❌ Отмена|отмена|cancel)$"), cancel_booking_flow)
        ],
        name="booking",
        persistent=False
    )