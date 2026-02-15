"""Главный файл бота."""

import logging
import asyncio
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ConversationHandler, ContextTypes

from config import TELEGRAM_BOT_TOKEN, LOGIN_EMAIL, LOGIN_PASSWORD, REQUIRED_TG_GROUP_ID
from database import init_db
from auth import login
from proxy_manager import ProxyManager
from rank_detector import RankDetectorImproved
from parser import parse_loop
from registration import get_registration_handler
from booking import get_booking_conversation_handler
from booking_handler import BOOKING_TRIGGER, booking_trigger_handler, get_confirm_booking_handler
from booking_scheduler import init_scheduler
from handlers import register_user_handlers
from admin_handlers import register_admin_handlers

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ДИАГНОСТИКА ВХОДЯЩИХ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════════


async def log_all_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Логирует все входящие сообщения для диагностики."""
    if update.message:
        chat_type = update.message.chat.type
        chat_id = update.message.chat.id
        user = update.message.from_user
        text = update.message.text
        
        logger.info(
            f"📨 Сообщение получено:\n"
            f"   Тип чата: {chat_type}\n"
            f"   ID чата: {chat_id}\n"
            f"   От: {user.full_name} (@{user.username}, ID: {user.id})\n"
            f"   Текст: {text}"
        )
        
        # Проверяем триггер
        if BOOKING_TRIGGER.search(text or ""):
            logger.info(f"   ✅ Триггер бронирования обнаружен!")
            
            if chat_type in ["group", "supergroup"]:
                logger.info(f"   ℹ️  Это групповой чат")
                if chat_id == REQUIRED_TG_GROUP_ID:
                    logger.info(f"   ✅ Это нужная группа (ID совпадает)")
                else:
                    logger.warning(
                        f"   ⚠️  ID группы не совпадает!\n"
                        f"      Текущий: {chat_id}\n"
                        f"      Ожидается: {REQUIRED_TG_GROUP_ID}"
                    )


# ══════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ
# ══════════════════════════════════════════════════════════════


async def main():
    """Главная функция запуска бота."""
    logger.info("=" * 60)
    logger.info("🚀 Запуск бота мониторинга клуба MangaBuff")
    logger.info("=" * 60)
    
    # Инициализация БД
    await init_db()
    
    # Инициализация прокси-менеджера
    proxy_manager = ProxyManager(enabled=True)
    logger.info("✅ Прокси-менеджер инициализирован")
    
    # Авторизация на сайте
    logger.info("🔐 Авторизация на сайте...")
    session = login(LOGIN_EMAIL, LOGIN_PASSWORD, proxy_manager)
    
    if not session:
        logger.error("❌ Не удалось авторизоваться на сайте")
        return
    
    logger.info("✅ Авторизация успешна")
    
    # Инициализация детектора рангов
    rank_detector = RankDetectorImproved()
    if rank_detector.is_ready:
        stats = rank_detector.get_stats()
        logger.info(
            f"✅ Детектор рангов готов: {stats['total_templates']} шаблонов "
            f"для рангов {list(stats['ranks'].keys())}"
        )
    else:
        logger.warning("⚠️  Детектор рангов не готов (нет шаблонов)")
    
    # Создание Telegram-бота
    logger.info("🤖 Инициализация Telegram-бота...")
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Сохраняем сессию в bot_data для доступа из handlers
    application.bot_data["session"] = session
    application.bot_data["rank_detector"] = rank_detector
    
    # Регистрация handlers
    logger.info("📝 Регистрация обработчиков...")
    
    # 0. ДИАГНОСТИКА - логируем ВСЕ сообщения (самый низкий приоритет)
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            log_all_messages
        ),
        group=999  # Очень низкий приоритет - выполняется в конце
    )
    
    # 1. Регистрация
    application.add_handler(get_registration_handler())
    
    # 2. Пользовательские команды
    register_user_handlers(application)
    
    # 3. Команды администратора
    register_admin_handlers(application)

    # 4. FSM бронирования - ТОЛЬКО ДЛЯ ЛИЧНЫХ СООБЩЕНИЙ
    from booking import start_booking_flow, STEP_DATE, STEP_START_TIME, STEP_END_TIME
    from booking import receive_date, receive_start_time, receive_end_time, cancel_booking_flow

    booking_conv_private = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.TEXT & 
                filters.Regex(BOOKING_TRIGGER) & 
                filters.ChatType.PRIVATE &  # ТОЛЬКО личные сообщения
                ~filters.COMMAND,
                start_booking_flow
            )
        ],
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
        name="booking_private",
        persistent=False,
        per_chat=True,
        per_user=True,
        per_message=False
    )
    application.add_handler(booking_conv_private, group=0)
    
    # 5. ОТДЕЛЬНЫЙ ОБРАБОТЧИК ДЛЯ ГРУПП - без ConversationHandler
    async def handle_group_booking_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик триггера бронирования в группах."""
        logger.info("🔔 Триггер бронирования в группе обнаружен!")
        
        # Отправляем сообщение с инструкцией
        await update.message.reply_text(
            "📅 Для бронирования напиши мне в личные сообщения:\n"
            f"👉 @{context.bot.username}\n\n"
            "Или нажми /start и следуй инструкциям."
        )
        
        logger.info("✅ Ответ в группу отправлен")
    
    application.add_handler(
        MessageHandler(
            filters.TEXT & 
            filters.Regex(BOOKING_TRIGGER) & 
            (filters.ChatType.GROUP | filters.ChatType.SUPERGROUP) &  # ТОЛЬКО группы
            ~filters.COMMAND,
            handle_group_booking_trigger
        ),
        group=0  # Тот же приоритет, что и ConversationHandler
    )
    
    # 6. Callback для подтверждения брони
    application.add_handler(get_confirm_booking_handler())
    
    logger.info("✅ Обработчики зарегистрированы")
    logger.info("")
    logger.info("=" * 60)
    logger.info("⚠️  КРИТИЧЕСКИ ВАЖНО ДЛЯ РАБОТЫ В ГРУППАХ:")
    logger.info("=" * 60)
    logger.info("1. Откройте @BotFather")
    logger.info("2. Отправьте: /mybots")
    logger.info("3. Выберите вашего бота")
    logger.info("4. Bot Settings → Group Privacy → Turn off")
    logger.info("=" * 60)
    logger.info("")
    
    # Инициализация планировщика броней
    scheduler = init_scheduler(application.bot)
    
    # Запуск бота
    logger.info("🚀 Запуск Telegram-бота...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)

    logger.info("✅ Бот запущен и готов к работе")

    # Запуск фонового парсера
    logger.info("🔄 Запуск фонового парсера...")
    parse_task = asyncio.create_task(
        parse_loop(session, application.bot, rank_detector)
    )

    logger.info("=" * 60)
    logger.info("✅ ВСЕ СИСТЕМЫ ЗАПУЩЕНЫ")
    logger.info("=" * 60)

    try:
        # Ожидаем завершения парсера (бот уже запущен через start_polling)
        await parse_task
    except KeyboardInterrupt:
        logger.info("⏹ Получен сигнал остановки")
    finally:
        # Остановка планировщика
        scheduler.shutdown()
        logger.info("⏹ Планировщик остановлен")
        
        # Остановка бота
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        logger.info("⏹ Бот остановлен")
        
        # Закрытие сессии
        if hasattr(session, '_session'):
            session._session.close()
        else:
            session.close()
        logger.info("⏹ Сессия закрыта")
        
        logger.info("=" * 60)
        logger.info("👋 Бот завершил работу")
        logger.info("=" * 60)


# ══════════════════════════════════════════════════════════════
# ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════════


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹ Программа прервана пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)