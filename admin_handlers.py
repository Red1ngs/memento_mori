"""Команды администратора."""

import logging
from datetime import datetime, timedelta
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
from weekly_stats import (
    get_week_contributions_from_db,
    get_available_weeks,
    format_weekly_message,
    format_week_range,
    get_week_start,
    get_week_end,
    ensure_weekly_tables,
)
from alliance_weekly_stats import (
    get_alliance_week_rows,
    get_alliance_available_weeks,
    format_alliance_weekly_message,
    format_alliance_week_range,
    get_alliance_week_start,
    get_alliance_week_end,
    ensure_alliance_weekly_tables,
    clear_pinned_alliance_message,
    send_or_update_alliance_pinned,
)

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
        bookings = await get_all_booking_history(limit=50)
        text = format_all_history(bookings)
    else:
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
        status_text = {
            "confirmed": "уже подтверждена",
            "cancelled": "отменена",
            "cancelled_by_user": "отменена",
            "cancelled_by_admin": "отменена администратором",
            "completed": "завершена"
        }.get(booking.status, "неактивна")

        await update.message.reply_text(
            f"❌ Бронь #{booking_id} уже неактивна (статус: {booking.status})."
        )
        return

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

    bot = context.bot
    await send_booking_cancelled_to_user(bot, booking)
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
# КОМАНДА НЕДЕЛЬНОЙ СТАТИСТИКИ
# ══════════════════════════════════════════════════════════════


@admin_only
async def weekstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает статистику вкладов за неделю из БД.

    Использование:
    - /weekstats           — текущая неделя
    - /weekstats YYYY-MM-DD — неделя, содержащая эту дату
    - /weekstats list      — список всех доступных недель
    """
    await ensure_weekly_tables()

    arg = context.args[0] if context.args else None

    # ── Список доступных недель ──────────────────────────────
    if arg and arg.lower() == "list":
        weeks = await get_available_weeks()
        if not weeks:
            await update.message.reply_text("📋 В БД пока нет данных о вкладах.")
            return

        lines = []
        for ws in weeks:
            we = get_week_end(ws)
            s = datetime.strptime(ws, "%Y-%m-%d")
            e = datetime.strptime(we, "%Y-%m-%d")
            lines.append(
                f"• {s.day:02d}.{s.month:02d} — {e.day:02d}.{e.month:02d}  "
                f"(запрос: /weekstats {ws})"
            )

        text = f"📅 Доступные недели ({len(weeks)}):\n\n" + "\n".join(lines)
        await update.message.reply_text(text)
        return

    # ── Определяем целевую неделю ────────────────────────────
    if arg:
        # Парсим любую дату и берём понедельник её недели
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            week_start = get_week_start(dt)
        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используй YYYY-MM-DD\n"
                "Пример: /weekstats 2026-02-16\n"
                "Или: /weekstats list — список всех недель"
            )
            return
    else:
        week_start = get_week_start()

    # ── Получаем данные из БД ────────────────────────────────
    contributions = await get_week_contributions_from_db(week_start)

    week_end = get_week_end(week_start)
    s = datetime.strptime(week_start, "%Y-%m-%d")
    e = datetime.strptime(week_end, "%Y-%m-%d")
    range_str = f"{s.day:02d}.{s.month:02d} — {e.day:02d}.{e.month:02d}"

    if not contributions:
        await update.message.reply_text(
            f"📊 Статистика за неделю {range_str}\n\n"
            f"Данных нет. Проверь: /weekstats list"
        )
        return

    # Формируем текст с номерами строк
    total = sum(c["contribution"] for c in contributions)
    lines = [f"📊 <b>Статистика вкладов</b> ({range_str})\n"]

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for i, c in enumerate(contributions, 1):
        prefix = medals.get(i, f"{i}.")
        url = c.get("profile_url", "")
        name = f'<a href="{url}">{c["nick"]}</a>' if url else c["nick"]
        lines.append(f"{prefix} {name} — <b>{c['contribution']}</b>")

    lines.append(f"\n👥 Всего участников: {len(contributions)}")
    lines.append(f"🔢 Всего вкладов: <b>{total}</b>")

    text = "\n".join(lines)

    # Разбиваем на части при необходимости
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await update.message.reply_text(part, parse_mode="HTML",
                                            disable_web_page_preview=True)
    else:
        await update.message.reply_text(text, parse_mode="HTML",
                                        disable_web_page_preview=True)

    logger.info(
        f"Администратор запросил статистику за неделю {week_start} "
        f"({len(contributions)} участников)"
    )


@admin_only
async def alliancestats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает статистику вкладов клуба в альянс за неделю из БД.

    Использование:
    - /alliancestats           — текущая неделя
    - /alliancestats YYYY-MM-DD — неделя, содержащая эту дату
    - /alliancestats list      — список всех доступных недель
    """
    await ensure_alliance_weekly_tables()

    arg = context.args[0] if context.args else None

    # ── Список доступных недель ──────────────────────────────
    if arg and arg.lower() == "list":
        weeks = await get_alliance_available_weeks()
        if not weeks:
            await update.message.reply_text("📋 В БД пока нет данных о вкладах в альянс.")
            return

        lines = []
        for ws in weeks:
            we = get_alliance_week_end(ws)
            s = datetime.strptime(ws, "%Y-%m-%d")
            e = datetime.strptime(we, "%Y-%m-%d")
            lines.append(
                f"• {s.day:02d}.{s.month:02d} — {e.day:02d}.{e.month:02d}  "
                f"(запрос: /alliancestats {ws})"
            )

        text = f"📅 Доступные недели ({len(weeks)}):\n\n" + "\n".join(lines)
        await update.message.reply_text(text)
        return

    # ── Определяем целевую неделю ────────────────────────────
    if arg:
        try:
            dt = datetime.strptime(arg, "%Y-%m-%d")
            week_start = get_alliance_week_start(dt)
        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используй YYYY-MM-DD\n"
                "Пример: /alliancestats 2026-02-17\n"
                "Или: /alliancestats list — список всех недель"
            )
            return
    else:
        week_start = get_alliance_week_start()

    # ── Получаем данные из БД ────────────────────────────────
    rows = await get_alliance_week_rows(week_start)

    week_end = get_alliance_week_end(week_start)
    s = datetime.strptime(week_start, "%Y-%m-%d")
    e = datetime.strptime(week_end, "%Y-%m-%d")
    range_str = f"{s.day:02d}.{s.month:02d} — {e.day:02d}.{e.month:02d}"

    if not rows:
        await update.message.reply_text(
            f"📊 Вклады в альянс за неделю {range_str}\n\n"
            f"Данных нет. Проверь: /alliancestats list"
        )
        return

    # Считаем суммарный прирост
    total_delta  = sum(r["contribution_current"] - r["contribution_baseline"] for r in rows)
    total_curr   = sum(r["contribution_current"] for r in rows)
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}

    lines = [f"🏰 <b>Вклад клуба в альянс</b> ({range_str})\n"]
    lines.append(
        "<code>№   Ник                  Старт → Сейчас  Прирост</code>"
    )

    for i, r in enumerate(rows, 1):
        prefix    = medals.get(i, f"{i}.")
        url       = r.get("profile_url", "")
        name      = f'<a href="{url}">{r["nick"]}</a>' if url else r["nick"]
        base      = r["contribution_baseline"]
        curr      = r["contribution_current"]
        delta     = curr - base
        delta_str = f"+{delta}" if delta >= 0 else str(delta)

        lines.append(
            f"{prefix} {name}\n"
            f"   {base} → <b>{curr}</b>  ({delta_str})"
        )

    lines.append(f"\n👥 Участников: {len(rows)}")
    lines.append(f"📈 Прирост за неделю: <b>+{total_delta}</b>")
    lines.append(f"🔢 Итого вкладов сейчас: <b>{total_curr}</b>")

    text = "\n".join(lines)

    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await update.message.reply_text(
                part, parse_mode="HTML", disable_web_page_preview=True
            )
    else:
        await update.message.reply_text(
            text, parse_mode="HTML", disable_web_page_preview=True
        )

    logger.info(
        f"Администратор запросил вклады в альянс за {week_start} "
        f"({len(rows)} участников, прирост +{total_delta})"
    )


@admin_only
async def refreshalliance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Принудительно пересоздаёт закреплённое сообщение вкладов клуба в альянс.
    Полезно если сообщение было удалено или бот потерял message_id.
    """
    from config import REQUIRED_TG_GROUP_ID

    await update.message.reply_text("⏳ Обновляю сообщение вкладов в альянс...")

    await clear_pinned_alliance_message(REQUIRED_TG_GROUP_ID)

    week_start = get_alliance_week_start()
    rows = await get_alliance_week_rows(week_start)

    if not rows:
        await update.message.reply_text(
            "⚠️ Нет данных за текущую неделю в БД.\n"
            "Данные обновятся при следующей проверке альянса."
        )
        return

    await send_or_update_alliance_pinned(context.bot, rows, week_start)
    await update.message.reply_text("✅ Закреплённое сообщение альянса обновлено.")


@admin_only
async def refreshweekly_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Принудительно обновляет закреплённое сообщение недельной статистики.
    Полезно если бот потерял message_id или сообщение было удалено.
    """
    from weekly_stats import (
        get_week_contributions_from_db,
        send_or_update_weekly_pinned,
        clear_pinned_message_info,
        get_week_start,
    )
    from config import REQUIRED_TG_GROUP_ID

    await update.message.reply_text("⏳ Обновляю закреплённое сообщение...")

    # Сбрасываем сохранённый message_id чтобы отправить новое сообщение
    await clear_pinned_message_info(REQUIRED_TG_GROUP_ID)

    week_start = get_week_start()
    contributions = await get_week_contributions_from_db(week_start)

    if not contributions:
        await update.message.reply_text(
            "⚠️ Нет данных за текущую неделю в БД.\n"
            "Данные обновятся при следующем парсинге."
        )
        return

    await send_or_update_weekly_pinned(context.bot, contributions, week_start)
    await update.message.reply_text("✅ Закреплённое сообщение обновлено.")


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
    application.add_handler(CommandHandler("weekstats", weekstats_command))
    application.add_handler(CommandHandler("refreshweekly", refreshweekly_command))
    application.add_handler(CommandHandler("alliancestats", alliancestats_command))
    application.add_handler(CommandHandler("refreshalliance", refreshalliance_command))

    logger.info("✅ Команды администратора зарегистрированы")