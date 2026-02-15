"""Конфигурация проекта."""

import os
import pytz
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# ══════════════════════════════════════════════════════════════
# САЙТ
# ══════════════════════════════════════════════════════════════

BASE_URL = os.getenv("BASE_URL", "https://mangabuff.ru")
CLUB_BOOST_PATH = os.getenv("CLUB_BOOST_PATH", "")
CLUB_PAGE_PATH = os.getenv("CLUB_PAGE_PATH", "")

# ══════════════════════════════════════════════════════════════
# АВТОРИЗАЦИЯ
# ══════════════════════════════════════════════════════════════

LOGIN_EMAIL = os.getenv("LOGIN_EMAIL", "")
LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD", "")

# ══════════════════════════════════════════════════════════════
# ПРОКСИ
# ══════════════════════════════════════════════════════════════

PROXY_COUNTRIES = os.getenv("PROXY_COUNTRIES", "RU,UA,BY").split(",")

# ══════════════════════════════════════════════════════════════
# ПАРСИНГ
# ══════════════════════════════════════════════════════════════

PARSE_INTERVAL_SECONDS = int(os.getenv("PARSE_INTERVAL_SECONDS", "1"))

# ══════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID", "0"))
REQUIRED_TG_GROUP_ID = int(os.getenv("REQUIRED_TG_GROUP_ID", "0"))

# ══════════════════════════════════════════════════════════════
# ЧАСОВОЙ ПОЯС
# ══════════════════════════════════════════════════════════════

TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")
TZ = pytz.timezone(TIMEZONE)


def now_msk() -> datetime:
    """Текущее время в МСК. Использовать везде вместо datetime.now()."""
    return datetime.now(TZ)


# ══════════════════════════════════════════════════════════════
# БРОНИРОВАНИЕ
# ══════════════════════════════════════════════════════════════

BOOKING_MAX_HOURS = int(os.getenv("BOOKING_MAX_HOURS", "2"))
BOOKING_CONFIRM_BEFORE_MINUTES = int(os.getenv("BOOKING_CONFIRM_BEFORE_MINUTES", "5"))
BOOKING_CONFIRM_GRACE_MINUTES = int(os.getenv("BOOKING_CONFIRM_GRACE_MINUTES", "5"))

# ══════════════════════════════════════════════════════════════
# HTTP
# ══════════════════════════════════════════════════════════════

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
