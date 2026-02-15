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
                return None
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Извлекаем данные
            card_id = self._extract_card_id(soup)
            if not card_id:
                logger.error("Не удалось извлечь card_id")
                return None
            
            card_name = self._extract_card_name(soup)
            card_image_url = self._extract_card_image(soup)
            replacements = self._extract_replacements(soup)
            daily_donated = self._extract_daily_donated(soup)
            wants_count = self._extract_wants_count(soup)
            owners_count = self._extract_owners_count(soup)
            club_owners = self._extract_club_owners(soup)
            
            # НЕ определяем ранг здесь - это будет делаться в parse_loop только при смене карты
            
            return {
                "card_id": card_id,
                "card_name": card_name,
                "card_rank": "?",  # Будет установлен позже в parse_loop
                "card_image_url": card_image_url,
                "replacements": replacements,
                "daily_donated": daily_donated,
                "wants_count": wants_count,
                "owners_count": owners_count,
                "club_owners": club_owners,
                "discovered_at": ts_for_db(now_msk())
            }
            
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}", exc_info=True)
            return None
    
    def _extract_card_id(self, soup: BeautifulSoup) -> Optional[int]:
        """Извлекает ID карты из ссылки /cards/{id}/users."""
        link = soup.select_one('a[href*="/cards/"][href*="/users"]')
        if link:
            href = link.get("href", "")
            match = re.search(r'/cards/(\d+)/users', href)
            if match:
                return int(match.group(1))
        return None
    
    def _extract_card_name(self, soup: BeautifulSoup) -> str:
        """Извлекает название карты."""
        # Пробуем найти в alt изображения
        img = soup.select_one('.club-boost__image img')
        if img:
            alt = img.get("alt", "").strip()
            if alt:
                return alt
        
        # Пробуем найти в тексте рядом с изображением
        container = soup.select_one('.club-boost__image')
        if container:
            text = container.get_text(strip=True)
            if text:
                return text
        
        return "Неизвестная карта"
    
    def _extract_card_image(self, soup: BeautifulSoup) -> str:
        """Извлекает URL изображения карты."""
        img = soup.select_one('.club-boost__image img')
        if img:
            src = img.get("src", "")
            if src:
                # Если относительный URL, делаем абсолютным
                if src.startswith("/"):
                    return f"{BASE_URL}{src}"
                return src
        return ""
    
    def _extract_replacements(self, soup: BeautifulSoup) -> str:
        """Извлекает информацию о заменах (7/10)."""
        # Ищем паттерн "X / Y" где X - текущие замены, Y - лимит
        text = soup.get_text()
        match = re.search(r'(\d+)\s*/\s*(\d+)', text)
        if match:
            return f"{match.group(1)}/{match.group(2)}"
        return "0/10"
    
    def _extract_daily_donated(self, soup: BeautifulSoup) -> str:
        """Извлекает информацию о вложениях (82/50)."""
        # Ищем текст с вложениями
        text = soup.get_text()
        # Паттерн для поиска вложений (обычно больше число)
        matches = re.findall(r'(\d+)\s*/\s*(\d+)', text)
        if len(matches) >= 2:
            # Берём второе совпадение (первое - замены)
            return f"{matches[1][0]}/{matches[1][1]}"
        return "0/50"
    
    def _extract_wants_count(self, soup: BeautifulSoup) -> int:
        """Извлекает количество желающих карту."""
        # Ищем элемент с количеством желающих
        wants = soup.select_one('.club-boost__wants, .wants-count')
        if wants:
            text = wants.get_text(strip=True)
            match = re.search(r'\d+', text)
            if match:
                return int(match.group())
        return 0
    
    def _extract_owners_count(self, soup: BeautifulSoup) -> int:
        """Извлекает количество владельцев карты."""
        # Считаем элементы в списке владельцев
        owners = soup.select('.club-boost__owners-list .club-boost__user')
        return len(owners)
    
    def _extract_club_owners(self, soup: BeautifulSoup) -> List[int]:
        """Извлекает список ID владельцев карты из клуба."""
        owner_ids = []
        
        # Ищем все ссылки на пользователей в блоке владельцев
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
    Основной цикл парсинга.
    
    Args:
        session: авторизованная сессия
        bot: экземпляр Telegram бота
        rank_detector: детектор рангов
    """
    from database import get_current_card, archive_card, insert_card
    from notifier import notify_owners
    
    parser = BoostPageParser(session, rank_detector)
    logger.info("🔄 Запущен цикл парсинга страницы boost")
    
    while True:
        try:
            # Получаем текущую карту из БД перед парсингом
            current = await get_current_card()
            
            # Парсим страницу (без определения ранга)
            data = parser.parse()
            
            if data:
                # Проверяем, изменилась ли карта
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
                    
                    # Добавляем новую карту
                    await insert_card(data)
                    
                    # Отправляем уведомления
                    await notify_owners(bot, data)
                    
                    logger.info(
                        f"✅ Новая карта: {data['card_name']} "
                        f"(ID: {data['card_id']}, Ранг: {data['card_rank']})"
                    )
            
        except Exception as e:
            logger.error(f"Ошибка в цикле парсинга: {e}", exc_info=True)
        
        # Ждём перед следующей итерацией
        await asyncio.sleep(PARSE_INTERVAL_SECONDS)
