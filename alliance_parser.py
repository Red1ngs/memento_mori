"""
Парсер мониторинга альянса.

Мониторит:
1. Смену текущей манги (как раньше).
2. Вклады клуба Memento Mori (data-page="club64") с отображением
   прироста за неделю в закреплённом сообщении.
"""

import asyncio
import logging
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

from config import BASE_URL, ALLIANCE_URL, ALLIANCE_CHECK_INTERVAL
from timezone_utils import ts_for_db, now_msk
from alliance_weekly_stats import (
    CLUB_PAGE_ATTR,
    parse_alliance_club_contributions,
    compute_alliance_hash,
    get_alliance_week_start,
    get_alliance_week_rows,
    upsert_alliance_contributions,
    send_or_update_alliance_pinned,
)

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

    # ── Получение HTML страницы ──────────────────────────────

    def fetch_page(self) -> Optional[str]:
        """
        Загружает HTML страницы альянса.

        Returns:
            HTML-строка или None при ошибке.
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(ALLIANCE_URL, timeout=15)

                if response.status_code == 500:
                    logger.warning(
                        f"[Alliance] HTTP 500 (попытка {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    if attempt < self.MAX_RETRIES - 1:
                        import time; time.sleep(self.RETRY_DELAY)
                    continue

                if response.status_code != 200:
                    logger.warning(
                        f"[Alliance] HTTP {response.status_code} "
                        f"(попытка {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    if attempt < self.MAX_RETRIES - 1:
                        import time; time.sleep(self.RETRY_DELAY)
                    continue

                return response.text

            except requests.exceptions.Timeout:
                logger.warning(
                    f"[Alliance] Таймаут (попытка {attempt + 1}/{self.MAX_RETRIES})"
                )
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)
            except requests.exceptions.ConnectionError:
                logger.warning(
                    f"[Alliance] Ошибка соединения (попытка {attempt + 1}/{self.MAX_RETRIES})"
                )
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)
            except Exception as e:
                logger.error(f"[Alliance] Ошибка загрузки: {e}", exc_info=True)
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)

        return None

    # ── Парсинг slug текущей манги ────────────────────────────

    def get_current_manga_slug(self, html: str) -> Optional[str]:
        """Извлекает slug текущей манги из уже загруженного HTML."""
        try:
            soup = BeautifulSoup(html, "html.parser")

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
                    try:
                        img_url = style.split("url('")[1].split("'")[0]
                        return img_url.split("/posters/")[-1].replace(".jpg", "")
                    except IndexError:
                        pass

            return None

        except Exception as e:
            logger.error(f"[Alliance] Ошибка парсинга slug: {e}")
            return None

    # ── Детальные данные о манге ──────────────────────────────

    def get_manga_details(self, manga_slug: str) -> Optional[Dict[str, Any]]:
        """Получает детальную информацию о манге по slug."""
        for attempt in range(self.MAX_RETRIES):
            try:
                url = f"{BASE_URL}/manga/{manga_slug}"
                response = self.session.get(url, timeout=15)

                if response.status_code not in (200,):
                    if attempt < self.MAX_RETRIES - 1:
                        import time; time.sleep(self.RETRY_DELAY)
                    continue

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
                    "slug":          manga_slug,
                    "title":         title,
                    "image":         img_src,
                    "url":           f"{BASE_URL}/manga/{manga_slug}",
                    "discovered_at": ts_for_db(now_msk()),
                }

            except Exception as e:
                logger.error(
                    f"[Alliance] Ошибка деталей манги {manga_slug}: {e}",
                    exc_info=True
                )
                if attempt < self.MAX_RETRIES - 1:
                    import time; time.sleep(self.RETRY_DELAY)

        return None


# ══════════════════════════════════════════════════════════════
# ОСНОВНОЙ ЦИКЛ МОНИТОРИНГА
# ══════════════════════════════════════════════════════════════


async def alliance_monitor_loop(session: requests.Session, bot):
    """
    Фоновый цикл мониторинга альянса.

    Параллельно ведёт:
    1. Детектирование смены манги → уведомление в топик альянса.
    2. Мониторинг вкладов клуба (data-page="club64") →
       закреплённое сообщение с приростом за неделю.

    Args:
        session: авторизованная сессия requests
        bot:     экземпляр Telegram бота
    """
    from database import get_current_alliance_manga, save_alliance_manga
    from notifier import notify_alliance_manga_changed

    parser = AllianceParser(session)
    logger.info("🔄 Запущен мониторинг альянса (манга + вклады клуба)")

    loop = asyncio.get_event_loop()

    # ── Стартовое состояние ──────────────────────────────────

    # Загружаем страницу один раз при старте
    start_html = await loop.run_in_executor(None, parser.fetch_page)

    current_slug: Optional[str] = None
    if start_html:
        current_slug = parser.get_current_manga_slug(start_html)

    # Восстанавливаем slug манги из БД
    saved = await get_current_alliance_manga()

    if saved is None and current_slug and start_html:
        manga_info = await loop.run_in_executor(
            None, parser.get_manga_details, current_slug
        )
        if manga_info:
            await save_alliance_manga(manga_info)
            await notify_alliance_manga_changed(bot, manga_info, is_startup=True)
            logger.info(f"🚀 Стартовый тайтл альянса: {manga_info['title']}")
    elif saved:
        current_slug = saved["slug"]
        logger.info(f"🔖 Тайтл альянса из БД: {saved['title']}")

    # ── Состояние мониторинга вкладов ────────────────────────

    last_club_hash:  Optional[str] = None
    last_week_start: str           = get_alliance_week_start()
    is_initialized:  bool          = False   # флаг первого успешного снимка

    # Стартовая инициализация вкладов из первой загрузки
    if start_html:
        contributions = parse_alliance_club_contributions(start_html)
        if contributions:
            await upsert_alliance_contributions(
                last_week_start, contributions, is_new_week=True
            )
            rows = await get_alliance_week_rows(last_week_start)
            await send_or_update_alliance_pinned(bot, rows, last_week_start)
            last_club_hash = compute_alliance_hash(contributions)
            is_initialized = True
            logger.info(
                f"🚀 Старт мониторинга вкладов клуба: "
                f"{len(contributions)} участников, неделя {last_week_start}"
            )

    # ── Основной цикл ─────────────────────────────────────────

    check_count = 0

    while True:
        try:
            await asyncio.sleep(ALLIANCE_CHECK_INTERVAL)
            check_count += 1

            html = await loop.run_in_executor(None, parser.fetch_page)
            if not html:
                if check_count % 60 == 0:
                    logger.warning("[Alliance] Не удалось загрузить страницу")
                continue

            current_week_start = get_alliance_week_start()

            # ══════════════════════════════════════════════════
            # СМЕНА МАНГИ
            # ══════════════════════════════════════════════════

            new_slug = parser.get_current_manga_slug(html)
            if new_slug and new_slug != current_slug:
                logger.info(
                    f"[Alliance] Смена тайтла: {current_slug} → {new_slug}"
                )
                manga_info = await loop.run_in_executor(
                    None, parser.get_manga_details, new_slug
                )
                if manga_info:
                    await save_alliance_manga(manga_info)
                    await notify_alliance_manga_changed(bot, manga_info, is_startup=False)
                    current_slug = new_slug
                    logger.info(
                        f"✅ Уведомление об альянсе отправлено: {manga_info['title']}"
                    )
                else:
                    current_slug = new_slug

            # ══════════════════════════════════════════════════
            # МОНИТОРИНГ ВКЛАДОВ КЛУБА
            # ══════════════════════════════════════════════════

            contributions = parse_alliance_club_contributions(html)
            if not contributions:
                if check_count % 60 == 0:
                    logger.debug("[Alliance] Вклады клуба не найдены")
                continue

            current_hash = compute_alliance_hash(contributions)

            # Смена недели
            if current_week_start != last_week_start:
                logger.info(
                    f"[Alliance] Новая неделя: "
                    f"{last_week_start} → {current_week_start}"
                )
                # Сохраняем текущие значения как baseline новой недели
                await upsert_alliance_contributions(
                    current_week_start, contributions, is_new_week=True
                )
                last_week_start = current_week_start
                last_club_hash  = None   # Сбрасываем для гарантированного обновления

            # Данные изменились
            if current_hash != last_club_hash:
                is_new = not is_initialized or current_week_start != last_week_start
                await upsert_alliance_contributions(
                    current_week_start,
                    contributions,
                    is_new_week=is_new,
                )
                rows = await get_alliance_week_rows(current_week_start)
                await send_or_update_alliance_pinned(bot, rows, current_week_start)
                last_club_hash = current_hash
                is_initialized = True

                # Находим топ-прироста для лога
                top = max(
                    rows,
                    key=lambda r: r["contribution_current"] - r["contribution_baseline"],
                    default=None,
                )
                if top:
                    delta = top["contribution_current"] - top["contribution_baseline"]
                    logger.info(
                        f"[Alliance] Вклады обновлены. "
                        f"Лидер прироста: {top['nick']} (+{delta})"
                    )
            elif check_count % 60 == 0:
                logger.debug(
                    f"[Alliance] Вклады без изменений (проверка #{check_count})"
                )

        except asyncio.CancelledError:
            logger.info("⏹ Мониторинг альянса остановлен")
            break
        except Exception as e:
            logger.error(f"[Alliance] Ошибка в цикле: {e}", exc_info=True)
            await asyncio.sleep(30)