"""Главный файл бота."""

import logging
import asyncio
from telegram.ext import Application, MessageHandler, filters, ConversationHandler

from config import TELEGRAM_BOT_TOKEN, LOGIN_EMAIL, LOGIN_PASSWORD
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
    
    # 1. Регистрация
    application.add_handler(get_registration_handler())
    
    # 2. Пользовательские команды
    register_user_handlers(application)
    
    # 3. Команды администратора
    register_admin_handlers(application)

    # 4. FSM бронирования (создаём с entry_points сразу)
    from booking import start_booking_flow, STEP_DATE, STEP_START_TIME, STEP_END_TIME, receive_date, receive_start_time, receive_end_time, cancel_booking_flow

    booking_conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.TEXT & filters.Regex(BOOKING_TRIGGER) & ~filters.COMMAND,
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
        name="booking",
        persistent=False,
        # ВАЖНО: Работает и в ЛС, и в группах
        per_chat=True,
        per_user=True
    )
    application.add_handler(booking_conv)
    
    # 6. Callback для подтверждения брони
    application.add_handler(get_confirm_booking_handler())
    
    logger.info("✅ Обработчики зарегистрированы")
    
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
