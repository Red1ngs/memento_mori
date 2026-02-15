"""Менеджер прокси для ротации."""

import logging
from typing import Optional, Dict
from fp.fp import FreeProxy
import requests

from config import PROXY_COUNTRIES, BASE_URL, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)


class ProxyManager:
    """Менеджер прокси с автоматической ротацией."""
    
    def __init__(self, enabled: bool = True):
        """
        Args:
            enabled: использовать ли прокси
        """
        self._enabled = enabled
        self._current_proxy: Optional[str] = None
        self._failed_proxies: set = set()
    
    def is_enabled(self) -> bool:
        """Проверяет, включены ли прокси."""
        return self._enabled
    
    def get_proxies(self) -> Optional[Dict[str, str]]:
        """
        Получает текущий прокси или подбирает новый.
        
        Returns:
            {"http": "...", "https": "..."} или None
        """
        if not self._enabled:
            return None
        
        # Если есть рабочий прокси, используем его
        if self._current_proxy and self._test_proxy(self._current_proxy):
            return self._format_proxy(self._current_proxy)
        
        # Иначе ищем новый
        return self._find_working_proxy()
    
    def rotate(self):
        """Принудительная ротация прокси."""
        if self._current_proxy:
            self._failed_proxies.add(self._current_proxy)
            logger.info(f"Прокси {self._current_proxy} помечен как неработающий")
        
        self._current_proxy = None
    
    def _find_working_proxy(self) -> Optional[Dict[str, str]]:
        """Ищет рабочий прокси из указанных стран."""
        max_attempts = 2
        
        for attempt in range(max_attempts):
            try:
                logger.info(f"Поиск прокси (попытка {attempt + 1}/{max_attempts})...")
                
                proxy = FreeProxy(
                    country_id=PROXY_COUNTRIES,
                    https=True
                ).get()
                
                # Пропускаем уже проваленные прокси
                if proxy in self._failed_proxies:
                    continue
                
                # Тестируем прокси
                if self._test_proxy(proxy):
                    self._current_proxy = proxy
                    logger.info(f"✅ Найден рабочий прокси: {proxy}")
                    return self._format_proxy(proxy)
                else:
                    self._failed_proxies.add(proxy)
                    
            except Exception as e:
                logger.warning(f"Ошибка при поиске прокси: {e}")
        
        logger.error("❌ Не удалось найти рабочий прокси")
        return None
    
    def _test_proxy(self, proxy: str) -> bool:
        """
        Тестирует прокси запросом к BASE_URL.
        
        Args:
            proxy: URL прокси
        
        Returns:
            True если прокси работает
        """
        proxies = self._format_proxy(proxy)
        
        try:
            response = requests.get(
                BASE_URL,
                proxies=proxies,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True
            )
            return response.status_code == 200
            
        except Exception:
            return False
    
    @staticmethod
    def _format_proxy(proxy: str) -> Dict[str, str]:
        """Форматирует прокси для requests."""
        return {"http": proxy, "https": proxy}
    
    def clear_failed(self):
        """Очищает список проваленных прокси."""
        self._failed_proxies.clear()
        logger.info("Список проваленных прокси очищен")
    
    def get_stats(self) -> Dict:
        """Возвращает статистику."""
        return {
            "enabled": self._enabled,
            "current_proxy": self._current_proxy,
            "failed_count": len(self._failed_proxies)
        }
