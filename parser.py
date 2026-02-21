"""Парсер страницы boost клуба."""

import logging
import asyncio
import re
from typing import Optional, Dict, Any, List
from bs4 import BeautifulSoup
import requests

from config import BASE_URL, CLUB_BOOST_PATH, PARSE_INTERVAL_SECONDS
from timezone_utils import ts_for_db, now_msk
from rank_detector import RankDetectorImproved
from weekly_stats import (
    parse_weekly_contributions,
    compute_stats_hash,
    save_weekly_contributions,
    send_or_update_weekly_pinned,
    get_week_start,
)

logger = logging.getLogger(__name__)


class BoostPageParser:
    """Парсер страницы boost клуба."""

    def __init__(self, session: requests.Session, rank_detector: RankDetectorImproved):
        """
        Args:
            session: авторизованная сессия
            rank_detector: детектор рангов карт
        """
        self.session = session
        self.rank_detector = rank_detector
        self.url = f"{BASE_URL}{CLUB_BOOST_PATH}"
        self._consecutive_errors = 0
        self._max_consecutive_errors = 5

    def parse(self) -> Optional[Dict[str, Any]]:
        """
        Парсит страницу boost.

        Returns:
            Словарь с данными карты или None при ошибке
        """
        try:
            response = self.session.get(self.url)

            if response.status_code != 200:
                logger.error(f"Ошибка загрузки страницы: {response.status_code}")
                self._mark_error()
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            # Извлекаем данные
            card_id = self._extract_card_id(soup)
            if not card_id:
                logger.error("Не удалось извлечь card_id")
                self._mark_error()
                return None

            card_image_url = self._extract_card_image(soup)
            replacements = self._extract_replacements(soup)
            daily_donated = self._extract_daily_donated(soup)
            club_owners = self._extract_club_owners(soup)

            # Успешный парсинг - сбрасываем счётчик ошибок
            self._mark_success()

            return {
                "card_id":        card_id,
                "card_rank":      "?",  # Будет установлен позже в parse_loop
                "card_image_url": card_image_url,
                "replacements":   replacements,
                "daily_donated":  daily_donated,
                "club_owners":    club_owners,
                "discovered_at":  ts_for_db(now_msk()),
            }

        except (requests.exceptions.ProxyError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            self._mark_error()
            logger.error(f"Ошибка сети при парсинге: {type(e).__name__}")
            return None

        except Exception as e:
            self._mark_error()
            logger.error(f"Ошибка парсинга: {e}", exc_info=True)
            return None

    def fetch_weekly_ajax(self) -> Optional[str]:
        """
        Запрашивает AJAX-эндпоинт недельной статистики клуба.

        Вкладка «Неделя» на странице буста подгружается динамически:
        data-ajax="/clubs/getTopUsers?period=week"
        Страница возвращает HTML с .club-boost__top-item элементами.

        Returns:
            HTML-строка с топом недели или None при ошибке
        """
        url = f"{BASE_URL}/clubs/getTopUsers?period=week"

        # CSRF-токен берём из куки сессии (XSRF-TOKEN)
        xsrf_token = ""
        try:
            xsrf_token = self.session.cookies.get("XSRF-TOKEN", "")
            # Laravel URL-декодирует значение куки — нам нужна сырая строка
            import urllib.parse
            xsrf_token = urllib.parse.unquote(xsrf_token)
        except Exception:
            pass

        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRF-TOKEN":     xsrf_token,
            "Referer":          self.url,
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "Origin":           BASE_URL,
            "Content-Length":   "0",
        }

        try:
            resp = self.session.post(url, headers=headers, data={}, timeout=15)

            if resp.status_code != 200:
                logger.warning(
                    f"[Weekly AJAX] POST {url} → HTTP {resp.status_code}"
                )
                return None

            # Ответ — JSON вида {"content": "<html...>"}
            try:
                data = resp.json()
                content = data.get("content", "")
                if content:
                    logger.debug("[Weekly AJAX] Успешно получены данные (JSON→content)")
                    return content
                logger.warning("[Weekly AJAX] JSON без поля 'content'")
                return None
            except ValueError:
                # На случай если вернули сырой HTML (не JSON)
                if "club-boost__top" in resp.text:
                    logger.debug("[Weekly AJAX] Успешно получены данные (raw HTML)")
                    return resp.text
                logger.warning("[Weekly AJAX] Ответ не JSON и не содержит нужных данных")
                return None

        except Exception as e:
            logger.warning(f"[Weekly AJAX] Ошибка запроса: {e}")
            return None

    def _mark_success(self):
        """Отмечает успешный парсинг."""
        self._consecutive_errors = 0

    def _mark_error(self):
        """Отмечает ошибку парсинга."""
        self._consecutive_errors += 1
        if self._consecutive_errors >= self._max_consecutive_errors:
            logger.warning(
                f"⚠️ {self._consecutive_errors} ошибок парсинга подряд - "
                f"возможна проблема с прокси"
            )

    def _extract_card_id(self, soup: BeautifulSoup) -> Optional[int]:
        """Извлекает ID карты из ссылки /cards/{id}/users."""
        link = soup.select_one('a[href*="/cards/"][href*="/users"]')
        if link:
            href = link.get("href", "")
            match = re.search(r'/cards/(\d+)/users', href)
            if match:
                return int(match.group(1))
        return None

    def _extract_card_image(self, soup: BeautifulSoup) -> str:
        """Извлекает URL изображения карты."""
        img = soup.select_one('.club-boost__image img')
        if img:
            src = img.get("src", "")
            if src:
                if src.startswith("/"):
                    return f"{BASE_URL}{src}"
                return src
        return ""

    def _extract_replacements(self, soup: BeautifulSoup) -> str:
        """Извлекает информацию о заменах (7/10)."""
        text = soup.get_text()
        match = re.search(r'(\d+)\s*/\s*(\d+)', text)
        if match:
            return f"{match.group(1)}/{match.group(2)}"
        return "0/10"

    def _extract_daily_donated(self, soup: BeautifulSoup) -> str:
        """Извлекает информацию о вложениях (82/50)."""
        text = soup.get_text()
        matches = re.findall(r'(\d+)\s*/\s*(\d+)', text)
        if len(matches) >= 2:
            return f"{matches[1][0]}/{matches[1][1]}"
        return "0/50"

    def _extract_club_owners(self, soup: BeautifulSoup) -> List[int]:
        """Извлекает список ID владельцев карты из клуба."""
        owner_ids = []

        owners_block = soup.select_one('.club-boost__owners-list')
        if owners_block:
            links = owners_block.select('a[href*="/users/"]')
            for link in links:
                href = link.get("href", "")
                match = re.search(r'/users/(\d{1,7})', href)
                if match:
                    owner_ids.append(int(match.group(1)))

        return owner_ids


async def parse_loop(session: requests.Session, bot, rank_detector: RankDetectorImproved):
    """
    Основной цикл парсинга с мониторингом недельной статистики вкладов.

    Args:
        session: авторизованная сессия
        bot: экземпляр Telegram бота
        rank_detector: детектор рангов
    """
    from database import get_current_card, archive_card, insert_card
    from notifier import notify_owners, notify_group_new_card
    from card_info_parser import get_card_name, get_owners_nicknames

    parser = BoostPageParser(session, rank_detector)
    logger.info("🔄 Запущен цикл парсинга страницы boost")

    consecutive_failures = 0
    max_consecutive_failures = 5

    # ── Состояние недельной статистики ──────────────────────────
    last_weekly_hash: Optional[str] = None
    last_week_start: str = get_week_start()
    weekly_check_counter: int = 0
    WEEKLY_CHECK_EVERY = 10  # Проверяем AJAX каждые N итераций парсинга
    # ────────────────────────────────────────────────────────────

    while True:
        try:
            current = await get_current_card()
            data = parser.parse()

            if data:
                consecutive_failures = 0

                # ══════════════════════════════════════════════
                # МОНИТОРИНГ НЕДЕЛЬНОЙ СТАТИСТИКИ (AJAX)
                # ══════════════════════════════════════════════

                current_week_start = get_week_start()

                # Смена недели: сбрасываем хэш для обновления
                if current_week_start != last_week_start:
                    logger.info(
                        f"🗓 Новая неделя: {last_week_start} → {current_week_start}"
                    )
                    last_weekly_hash = None
                    last_week_start = current_week_start
                    weekly_check_counter = 0  # Принудительно проверяем сразу

                weekly_check_counter += 1
                # Запрашиваем AJAX не каждый цикл, чтобы не нагружать сервер
                if weekly_check_counter >= WEEKLY_CHECK_EVERY:
                    weekly_check_counter = 0
                    loop = asyncio.get_event_loop()
                    weekly_html = await loop.run_in_executor(
                        None, parser.fetch_weekly_ajax
                    )

                    if weekly_html:
                        weekly_contributions = parse_weekly_contributions(weekly_html)

                        if weekly_contributions:
                            current_hash = compute_stats_hash(weekly_contributions)

                            if current_hash != last_weekly_hash:
                                await save_weekly_contributions(
                                    current_week_start, weekly_contributions
                                )
                                await send_or_update_weekly_pinned(
                                    bot, weekly_contributions, current_week_start
                                )
                                last_weekly_hash = current_hash
                                logger.info(
                                    f"📊 Недельная статистика клуба обновлена "
                                    f"({len(weekly_contributions)} участников)"
                                )

                # ══════════════════════════════════════════════
                # МОНИТОРИНГ СМЕНЫ КАРТЫ
                # ══════════════════════════════════════════════

                if current is None or current.card_id != data["card_id"]:
                    logger.info(
                        f"🔄 Обнаружена смена карты: "
                        f"{current.card_id if current else 'None'} → {data['card_id']}"
                    )

                    # Определяем ранг только для новой карты
                    if data["card_image_url"] and rank_detector.is_ready:
                        data["card_rank"] = rank_detector.detect_from_url(
                            data["card_image_url"],
                            session=session
                        )
                    else:
                        data["card_rank"] = "?"

                    # Архивируем старую карту
                    if current:
                        await archive_card(current.id)

                    # Добавляем новую карту в БД
                    await insert_card(data)

                    loop = asyncio.get_event_loop()

                    # Название карты
                    card_name = await loop.run_in_executor(
                        None,
                        get_card_name,
                        session,
                        data["card_id"]
                    )

                    # Ники владельцев карты в клубе
                    owners_nicks = []
                    if data["club_owners"]:
                        owners_nicks = await loop.run_in_executor(
                            None,
                            get_owners_nicknames,
                            session,
                            data["club_owners"],
                            10
                        )

                    # Уведомляем владельцев в личку
                    await notify_owners(bot, data)

                    # Уведомляем группу в топик
                    await notify_group_new_card(bot, data, card_name, owners_nicks)

                    logger.info(
                        f"✅ Новая карта «{card_name}» "
                        f"ID {data['card_id']} (Ранг: {data['card_rank']}), "
                        f"владельцев: {len(owners_nicks)}"
                    )
            else:
                consecutive_failures += 1

                if consecutive_failures >= max_consecutive_failures:
                    logger.warning(
                        f"⚠️ {consecutive_failures} неудач парсинга подряд - "
                        f"пытаемся сменить прокси"
                    )

                    if hasattr(session, '_session'):
                        try:
                            proxy_manager = bot._application.bot_data.get("proxy_manager")
                            if proxy_manager:
                                proxy_manager.mark_failure()
                        except Exception as e:
                            logger.debug(f"Не удалось уведомить прокси-менеджер: {e}")

                    consecutive_failures = 0

        except Exception as e:
            logger.error(f"Ошибка в цикле парсинга: {e}", exc_info=True)
            consecutive_failures += 1

        await asyncio.sleep(PARSE_INTERVAL_SECONDS)