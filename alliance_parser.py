"""
Парсер мониторинга смены тайтла в альянсе.

Интегрирован в основной бот как фоновая задача asyncio.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

from config import BASE_URL, ALLIANCE_URL, ALLIANCE_CHECK_INTERVAL
from timezone_utils import ts_for_db, now_msk

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ПАРСЕР АЛЬЯНСА
# ══════════════════════════════════════════════════════════════


class AllianceParser:
    """Парсер страницы буста альянса."""

    MAX_RETRIES = 3
    RETRY_DELAY = 5

    def __init__(self, session: requests.Session):
        self.session = session

    def get_current_manga_slug(self) -> Optional[str]:
        """
        Получает slug текущей манги со страницы альянса.

        Returns:
            slug манги или None при ошибке
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(ALLIANCE_URL, timeout=15)

                if response.status_code == 500:
                    logger.warning(f"Ошибка сервера 500 (попытка {attempt + 1}/{self.MAX_RETRIES})")
                    if attempt < self.MAX_RETRIES - 1:
                        import time; time.sleep(self.RETRY_DELAY)
                        continue
                    return None

                if response.status_code != 200:
                    logger.warning(f"Ошибка получения страницы альянса: {response.status_code}")
                    if attempt < self.MAX_RETRIES - 1:
                        import time; time.sleep(self.RETRY_DELAY)
                        continue
                    return None

                soup = BeautifulSoup(response.text, "html.parser")

                # Вариант 1: ссылка card-show__placeholder
                manga_link = soup.find("a", class_="card-show__placeholder")
                if manga_link:
                    href = manga_link.get("href", "")
                    if href.startswith("/manga/"):
                        return href.replace("/manga/", "")

                # Вариант 2: background-image в card-show__header
                poster = soup.find("div", class_="card-show__header")
                if poster:
                    style = poster.get("style", "")
                    if "background-image: url(" in style:
                        img_url = style.split("url('")[1].split("'")[0]
                        return img_url.split("/posters/")[-1].replace(".jpg", "")

                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)
                    continue
                return None

            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут запроса (попытка {attempt + 1}/{self.MAX_RETRIES})")
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)
            except requests.exceptions.ConnectionError:
                logger.warning(f"Ошибка соединения (попытка {attempt + 1}/{self.MAX_RETRIES})")
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)
            except Exception as e:
                logger.error(f"Ошибка получения slug альянса: {e}", exc_info=True)
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)

        return None

    def get_manga_details(self, manga_slug: str) -> Optional[Dict[str, Any]]:
        """
        Получает детальную информацию о манге.

        Args:
            manga_slug: slug манги

        Returns:
            словарь с данными или None при ошибке
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                url = f"{BASE_URL}/manga/{manga_slug}"
                response = self.session.get(url, timeout=15)

                if response.status_code not in (200,):
                    logger.warning(
                        f"Ошибка получения страницы манги {manga_slug}: "
                        f"{response.status_code} (попытка {attempt + 1})"
                    )
                    if attempt < self.MAX_RETRIES - 1:
                        import time; time.sleep(self.RETRY_DELAY)
                        continue
                    return None

                soup = BeautifulSoup(response.text, "html.parser")

                # Название
                title = None
                for cls in ("manga-mobile__name", "manga__name"):
                    elem = soup.find("h1", class_=cls)
                    if elem:
                        title = elem.text.strip()
                        break
                if not title:
                    title = manga_slug

                # Изображение
                img_src = None
                img_elem = soup.find("img", class_="manga-mobile__image")
                if img_elem:
                    img_src = img_elem.get("src")
                if not img_src:
                    wrapper = soup.find("div", class_="manga__img")
                    if wrapper:
                        img = wrapper.find("img")
                        if img:
                            img_src = img.get("src")

                if img_src and img_src.startswith("/"):
                    img_src = f"{BASE_URL}{img_src}"

                return {
                    "slug": manga_slug,
                    "title": title,
                    "image": img_src,
                    "url": f"{BASE_URL}/manga/{manga_slug}",
                    "discovered_at": ts_for_db(now_msk())
                }

            except Exception as e:
                logger.error(f"Ошибка получения деталей манги {manga_slug}: {e}", exc_info=True)
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)

        return None


# ══════════════════════════════════════════════════════════════
# ОСНОВНОЙ ЦИКЛ МОНИТОРИНГА
# ══════════════════════════════════════════════════════════════


async def alliance_monitor_loop(session: requests.Session, bot):
    """
    Фоновый цикл мониторинга смены тайтла в альянсе.

    Запускается как asyncio.create_task в main.py.
    При смене манги отправляет уведомление в группу и сохраняет в БД.

    Args:
        session: авторизованная сессия requests
        bot: экземпляр Telegram бота
    """
    from database import get_current_alliance_manga, save_alliance_manga
    from notifier import notify_alliance_manga_changed

    parser = AllianceParser(session)
    logger.info("🔄 Запущен мониторинг альянса")

    # Получаем стартовое состояние
    loop = asyncio.get_event_loop()
    current_slug = await loop.run_in_executor(None, parser.get_current_manga_slug)

    # Сравниваем с последним сохранённым
    saved = await get_current_alliance_manga()
    if saved is None and current_slug:
        # Первый запуск — сохраняем стартовый тайтл и шлём уведомление
        manga_info = await loop.run_in_executor(None, parser.get_manga_details, current_slug)
        if manga_info:
            await save_alliance_manga(manga_info)
            await notify_alliance_manga_changed(bot, manga_info, is_startup=True)
            logger.info(f"🚀 Стартовый тайтл альянса: {manga_info['title']}")
    elif saved:
        current_slug = saved["slug"]
        logger.info(f"🔖 Тайтл альянса из БД: {saved['title']}")

    check_count = 0

    while True:
        try:
            await asyncio.sleep(ALLIANCE_CHECK_INTERVAL)
            check_count += 1

            new_slug = await loop.run_in_executor(None, parser.get_current_manga_slug)

            if not new_slug:
                if check_count % 60 == 0:
                    logger.warning("⚠️ Не удалось получить slug альянса")
                continue

            if new_slug != current_slug:
                logger.info(f"🔔 Смена тайтла альянса: {current_slug} → {new_slug}")

                manga_info = await loop.run_in_executor(
                    None, parser.get_manga_details, new_slug
                )

                if manga_info:
                    await save_alliance_manga(manga_info)
                    await notify_alliance_manga_changed(bot, manga_info, is_startup=False)
                    current_slug = new_slug
                    logger.info(f"✅ Уведомление об альянсе отправлено: {manga_info['title']}")
                else:
                    logger.warning(f"⚠️ Не удалось получить детали манги {new_slug}")
                    # Обновляем slug даже без деталей, чтобы не слать повтор
                    current_slug = new_slug

            elif check_count % 60 == 0:
                logger.debug(f"Alliance check #{check_count}: тайтл не изменился ({current_slug})")

        except asyncio.CancelledError:
            logger.info("⏹ Мониторинг альянса остановлен")
            break
        except Exception as e:
            logger.error(f"Ошибка в цикле мониторинга альянса: {e}", exc_info=True)
            await asyncio.sleep(30)