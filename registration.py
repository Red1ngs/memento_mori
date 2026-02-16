"""FSM регистрации пользователей."""

import asyncio
import logging
import re
from telegram import Update
from telegram.ext import (
    ContextTypes,
    ConversationHandler,
    CommandHandler,
    MessageHandler,
    filters
)

from config import REQUIRED_TG_GROUP_ID
from database import upsert_user, get_user
from club_parser import check_club_membership
from timezone_utils import ts_for_db, now_msk

logger = logging.getLogger(__name__)

# Состояния FSM
WAITING_FOR_URL = 1

# Regex для проверки URL MangaBuff
MANGABUFF_URL_PATTERN = re.compile(
    r'^https://mangabuff\.ru/users/(\d{1,7})$'
)


# ══════════════════════════════════════════════════════════════
# HANDLERS
# ══════════════════════════════════════════════════════════════


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user = update.effective_user

    # Проверяем, зарегистрирован ли уже
    db_user = await get_user(user.id)
    if db_user and db_user.is_verified:
        await update.message.reply_text(
            f"✅ Ты уже зарегистрирован!\n\n"
            f"👤 MangaBuff: {db_user.mangabuff_nick}\n"
            f"🔗 {db_user.mangabuff_url}\n\n"
            f"Используй /myaccount для просмотра данных."
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "👋 Привет! Я бот клуба на MangaBuff.\n\n"
        "Чтобы получать уведомления о картах клуба,\n"
        "привяжи свой аккаунт MangaBuff.\n\n"
        "Отправь ссылку на свой профиль в формате:\n"
        "https://mangabuff.ru/users/***"
    )

    return WAITING_FOR_URL


async def receive_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик получения URL профиля."""
    user = update.effective_user
    url = update.message.text.strip()

    # Валидация формата URL
    match = MANGABUFF_URL_PATTERN.match(url)
    if not match:
        await update.message.reply_text(
            "❌ Неверный формат ссылки.\n"
            "Отправь ссылку в формате:\n"
            "https://mangabuff.ru/users/***"
        )
        return WAITING_FOR_URL

    mangabuff_id = int(match.group(1))

    # Проверка 1: Членство в TG-группе
    try:
        member = await context.bot.get_chat_member(
            chat_id=REQUIRED_TG_GROUP_ID,
            user_id=user.id
        )

        if member.status not in ["member", "administrator", "creator"]:
            await update.message.reply_text(
                f"❌ Ты не состоишь в Telegram-группе клуба.\n\n"
                f"попробуй снова через /start"
            )
            return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка проверки членства в группе: {e}")
        await update.message.reply_text(
            "❌ Ошибка проверки членства в группе.\n"
            "Попробуй позже."
        )
        return ConversationHandler.END

    # Проверка 2: Членство в клубе на сайте
    await update.message.reply_text("⏳ Проверяю членство в клубе...")

    session = context.bot_data.get("session")
    if not session:
        await update.message.reply_text(
            "❌ Ошибка: сессия не инициализирована.\n"
            "Попробуй позже."
        )
        return ConversationHandler.END

    # check_club_membership синхронный (requests) — выносим в executor,
    # чтобы не блокировать event loop
    loop = asyncio.get_event_loop()
    try:
        is_member, mangabuff_nick = await loop.run_in_executor(
            None, check_club_membership, session, mangabuff_id
        )
    except Exception as e:
        logger.error(f"Ошибка при проверке членства в клубе: {e}")
        await update.message.reply_text(
            "❌ Ошибка при проверке членства в клубе.\n"
            "Попробуй позже."
        )
        return ConversationHandler.END

    if not is_member:
        await update.message.reply_text(
            f"❌ Аккаунт https://mangabuff.ru/users/{mangabuff_id}\n"
            f"не найден в клубе.\n\n"
            f"Убедись, что ты вступил в клуб и попробуй снова через /start"
        )
        return ConversationHandler.END

    # Обе проверки пройдены — сохраняем пользователя
    await upsert_user(
        tg_id=user.id,
        tg_username=user.username,
        tg_nickname=user.full_name,
        mangabuff_url=url,
        mangabuff_id=mangabuff_id,
        mangabuff_nick=mangabuff_nick or f"User{mangabuff_id}",
        is_verified=1,
        is_active=1,
        created_at=ts_for_db(now_msk())
    )

    await update.message.reply_text(
        f"✅ Аккаунт успешно привязан!\n\n"
        f"👤 MangaBuff: {mangabuff_nick or f'User{mangabuff_id}'}\n"
        f"🔗 {url}\n\n"
        f"Теперь ты будешь получать уведомления,\n"
        f"когда в клубе появится карта, которая есть у тебя."
    )

    logger.info(
        f"✅ Пользователь зарегистрирован: {user.full_name} "
        f"(TG: {user.id}, MB: {mangabuff_id})"
    )

    return ConversationHandler.END


async def cancel_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена регистрации."""
    await update.message.reply_text(
        "❌ Регистрация отменена.\n"
        "Используй /start для повторной попытки."
    )
    return ConversationHandler.END


# ══════════════════════════════════════════════════════════════
# CONVERSATION HANDLER
# ══════════════════════════════════════════════════════════════


def get_registration_handler() -> ConversationHandler:
    """Возвращает ConversationHandler для регистрации."""
    return ConversationHandler(
        entry_points=[
            CommandHandler("start", start_command)
        ],
        states={
            WAITING_FOR_URL: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    receive_url
                )
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel_registration)
        ],
        name="registration",
        persistent=False
    )