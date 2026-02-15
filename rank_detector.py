"""
детектор ранга карты
"""
import logging
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Tuple, List

import numpy as np
import requests
from PIL import Image

from config import REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════

RANKS_DIR: Path = Path(__file__).parent / 'ranks'
TARGET_SIZE: Tuple[int, int] = (288, 432)

# ────────────────────────────────────────────────────────────
# ЗОНЫ СРАВНЕНИЯ (x1, y1, x2, y2)
# ────────────────────────────────────────────────────────────
# ИСПРАВЛЕНО: Убрана left_frame - она захватывала artwork карты!

ZONES = {
    # Бейдж с буквой ранга (главная зона)
    'badge': (5, 5, 45, 45),      # 40×40 px - буква ранга
    
    # Верхняя часть рамки (цвет и текстура)
    'top_frame': (50, 5, 120, 25),  # 70×20 px - верхняя рамка
}

# Веса для каждой зоны при расчёте итогового MSE
ZONE_WEIGHTS = {
    'badge': 0.7,      # Буква - самое важное (70%)
    'top_frame': 0.3,  # Верхняя рамка (30%)
}

# Порог для определения ранга
MSE_THRESHOLD: float = 5000.0


# ══════════════════════════════════════════════════════════════
# ДЕТЕКТОР РАНГА
# ══════════════════════════════════════════════════════════════

class RankDetectorImproved:
    """
    Исправленный детектор ранга карты.
    
    Использует только badge и top_frame, игнорируя основное изображение карты.
    Поддерживает несколько вариантов дизайна одного ранга.
    """

    def __init__(self, ranks_dir: Path = RANKS_DIR):
        self.ranks_dir = ranks_dir
        # {"E": [template1, template2], "D": [template1], ...}
        # где каждый template = {'badge': array, 'top_frame': array}
        self.templates: Dict[str, List[Dict[str, np.ndarray]]] = {}
        self._load_templates()

    # ──────────────────────────────────────────────────────────
    # ЗАГРУЗКА ШАБЛОНОВ
    # ──────────────────────────────────────────────────────────

    def _load_templates(self):
        """Загружает все шаблоны из папки ranks/"""
        if not self.ranks_dir.exists():
            logger.warning(
                f"⚠️  Папка шаблонов не найдена: {self.ranks_dir}\n"
                "   Создайте папку 'ranks/' и добавьте frame-e.png, frame-d.png, frame-c.png"
            )
            return

        png_files = sorted(self.ranks_dir.glob('frame-*.png'))
        
        if not png_files:
            logger.warning("⚠️  В папке ranks/ нет файлов frame-*.png")
            return

        for fpath in png_files:
            # frame-e.png → "E"
            # frame-e-v2.png → "E" (игнорируем -v2)
            # frame-ss.png → "SS"
            name = fpath.stem.replace('frame-', '')
            rank = name.split('-')[0].upper()  # "e-v2" → "E"
            
            self._register(rank, str(fpath))

        if self.templates:
            total = sum(len(v) for v in self.templates.values())
            ranks_str = ', '.join(
                f"{r}({len(v)})" for r, v in sorted(self.templates.items())
            )
            logger.info(
                f"✅ Загружено {total} шаблонов для рангов: {ranks_str}"
            )
        else:
            logger.warning("⚠️  Шаблоны рангов не загружены")

    def _register(self, rank: str, filepath: str):
        """Регистрирует один шаблон (все зоны)"""
        try:
            img = Image.open(filepath).convert('RGB').resize(TARGET_SIZE)
            img_arr = np.array(img).astype(float)

            # Вырезаем все зоны из шаблона
            template = {
                zone_name: self._crop(img_arr, coords)
                for zone_name, coords in ZONES.items()
            }

            if rank not in self.templates:
                self.templates[rank] = []

            self.templates[rank].append(template)
            
            variant_num = len(self.templates[rank])
            logger.debug(
                f"Шаблон ранга {rank} вариант #{variant_num} загружен: {filepath}"
            )

        except Exception as e:
            logger.error(f"Ошибка загрузки шаблона {filepath}: {e}")

    # ──────────────────────────────────────────────────────────
    # ПУБЛИЧНОЕ API
    # ──────────────────────────────────────────────────────────

    def detect_from_url(
        self,
        image_url: str,
        session: Optional[requests.Session] = None,
    ) -> str:
        """Определяет ранг по URL изображения"""
        if not self.templates:
            logger.warning("Шаблоны не загружены")
            return "?"
        
        raw = self._download(image_url, session)
        if raw is None:
            return "?"
        
        return self._detect_from_bytes(raw)

    def detect_from_file(self, filepath: str) -> str:
        """Определяет ранг из локального файла"""
        if not self.templates:
            return "?"
        try:
            with open(filepath, 'rb') as f:
                return self._detect_from_bytes(f.read())
        except Exception as e:
            logger.error(f"detect_from_file({filepath}): {e}")
            return "?"

    def detect_from_bytes(self, image_bytes: bytes) -> str:
        """Определяет ранг из байтов"""
        if not self.templates:
            return "?"
        return self._detect_from_bytes(image_bytes)

    # ──────────────────────────────────────────────────────────
    # ОСНОВНАЯ ЛОГИКА РАСПОЗНАВАНИЯ
    # ──────────────────────────────────────────────────────────

    def _detect_from_bytes(self, raw: bytes) -> str:
        """Обработка байтов изображения"""
        try:
            img = Image.open(BytesIO(raw)).convert('RGB').resize(TARGET_SIZE)
            return self._run(np.array(img))
        except Exception as e:
            logger.error(f"Ошибка обработки изображения: {e}")
            return "?"

    def _run(self, card_arr: np.ndarray) -> str:
        """
        Главный метод распознавания.
        
        Сравнивает карту со всеми шаблонами всех рангов
        и выбирает наилучшее совпадение.
        """
        card_arr = card_arr.astype(float)
        
        # Вырезаем все зоны из карты
        card_zones = {
            zone_name: self._crop(card_arr, coords)
            for zone_name, coords in ZONES.items()
        }

        # Для каждого ранга находим минимальный MSE среди всех вариантов
        rank_scores: Dict[str, float] = {}
        rank_details: Dict[str, Dict] = {}  # для отладки

        for rank, templates_list in self.templates.items():
            # Считаем MSE для каждого варианта этого ранга
            variant_scores = []
            
            for variant_idx, template in enumerate(templates_list):
                # MSE по каждой зоне
                zone_mse = {}
                for zone_name in ZONES.keys():
                    diff = card_zones[zone_name] - template[zone_name]
                    zone_mse[zone_name] = float(np.mean(diff ** 2))
                
                # Взвешенное среднее MSE
                weighted_mse = sum(
                    zone_mse[zone] * ZONE_WEIGHTS[zone]
                    for zone in ZONES.keys()
                )
                
                variant_scores.append({
                    'mse': weighted_mse,
                    'zones': zone_mse,
                    'variant': variant_idx + 1
                })
            
            # Берём лучший вариант для этого ранга
            best_variant = min(variant_scores, key=lambda x: x['mse'])
            rank_scores[rank] = best_variant['mse']
            rank_details[rank] = best_variant

        # Выбираем ранг с минимальным MSE
        best_rank = min(rank_scores, key=rank_scores.__getitem__)
        best_mse = rank_scores[best_rank]
        best_details = rank_details[best_rank]

        # Логируем результаты
        self._log_results(rank_scores, rank_details, best_rank, best_mse)

        # Проверяем порог
        if best_mse > MSE_THRESHOLD:
            logger.warning(
                f"❌ Ранг не определён: лучший MSE {best_mse:.1f} > {MSE_THRESHOLD}\n"
                f"   Кандидат: {best_rank}\n"
                f"   Возможно, нужен шаблон для этого ранга"
            )
            return "?"

        logger.info(
            f"✅ Ранг определён: {best_rank} "
            f"(MSE={best_mse:.1f}, вариант {best_details['variant']})"
        )
        return best_rank

    def _log_results(
        self,
        rank_scores: Dict[str, float],
        rank_details: Dict[str, Dict],
        best_rank: str,
        best_mse: float
    ):
        """Подробное логирование результатов сравнения"""
        
        # Краткая сводка по всем рангам
        summary = "  ".join(
            f"{r}={mse:.0f}" for r, mse in sorted(rank_scores.items())
        )
        logger.debug(f"MSE по рангам: {summary}")

        # Детали по лучшему рангу
        details = rank_details[best_rank]
        zones_info = ", ".join(
            f"{zone}={mse:.0f}"
            for zone, mse in details['zones'].items()
        )
        logger.debug(
            f"Детали {best_rank}: вариант {details['variant']}, "
            f"зоны: {zones_info}"
        )

    # ──────────────────────────────────────────────────────────
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _crop(img_arr: np.ndarray, coords: Tuple[int, int, int, int]) -> np.ndarray:
        """Вырезает область из изображения"""
        x1, y1, x2, y2 = coords
        return img_arr[y1:y2, x1:x2]

    @staticmethod
    def _download(url: str, session: Optional[requests.Session]) -> Optional[bytes]:
        """Скачивает изображение"""
        try:
            r = (session or requests).get(url, timeout=REQUEST_TIMEOUT)
            return r.content if r.status_code == 200 else None
        except Exception as e:
            logger.error(f"Ошибка загрузки {url}: {e}")
            return None

    # ──────────────────────────────────────────────────────────
    # УТИЛИТЫ И ОТЛАДКА
    # ──────────────────────────────────────────────────────────

    @property
    def available_ranks(self) -> List[str]:
        """Список доступных рангов"""
        return sorted(self.templates.keys())

    @property
    def is_ready(self) -> bool:
        """Готов ли детектор к работе"""
        return bool(self.templates)

    def get_stats(self) -> Dict:
        """Статистика по загруженным шаблонам"""
        return {
            'total_templates': sum(len(v) for v in self.templates.values()),
            'ranks': {
                rank: len(variants)
                for rank, variants in self.templates.items()
            }
        }

    def debug_compare(
        self,
        image_url: str,
        session: Optional[requests.Session] = None
    ) -> Dict[str, Dict]:
        """
        Детальное сравнение для отладки.
        
        Возвращает:
        {
            'E': {'mse': 123.4, 'zones': {...}},
            'D': {'mse': 5432.1, 'zones': {...}},
            ...
        }
        """
        raw = self._download(image_url, session)
        if not raw:
            return {}

        try:
            img = Image.open(BytesIO(raw)).convert('RGB').resize(TARGET_SIZE)
            card_arr = np.array(img).astype(float)

            card_zones = {
                zone_name: self._crop(card_arr, coords)
                for zone_name, coords in ZONES.items()
            }

            results = {}
            for rank, templates_list in self.templates.items():
                variant_results = []
                
                for template in templates_list:
                    zone_mse = {
                        zone: float(np.mean((card_zones[zone] - template[zone]) ** 2))
                        for zone in ZONES.keys()
                    }
                    weighted = sum(
                        zone_mse[z] * ZONE_WEIGHTS[z] for z in ZONES.keys()
                    )
                    variant_results.append({
                        'mse': weighted,
                        'zones': zone_mse
                    })
                
                best = min(variant_results, key=lambda x: x['mse'])
                results[rank] = best

            return results

        except Exception as e:
            logger.error(f"debug_compare error: {e}")
            return {}


# ══════════════════════════════════════════════════════════════
# ОБРАТНАЯ СОВМЕСТИМОСТЬ
# ══════════════════════════════════════════════════════════════

# Алиас для совместимости со старым кодом
RankDetector = RankDetectorImproved


# ══════════════════════════════════════════════════════════════
# ТЕСТИРОВАНИЕ (если запускается как скрипт)
# ══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    detector = RankDetectorImproved()
    
    print("\n" + "=" * 60)
    print("ИСПРАВЛЕННЫЙ ДЕТЕКТОР РАНГОВ")
    print("=" * 60)
    
    if detector.is_ready:
        stats = detector.get_stats()
        print(f"\n✅ Готов к работе!")
        print(f"   Всего шаблонов: {stats['total_templates']}")
        print(f"   Ранги: {', '.join(f'{r}({n})' for r, n in stats['ranks'].items())}")
        
        print("\nЗоны сравнения:")
        for zone, coords in ZONES.items():
            weight = ZONE_WEIGHTS[zone]
            print(f"  • {zone}: {coords} (вес {weight*100:.0f}%)")
        
        print(f"\nПорог MSE: {MSE_THRESHOLD}")
    else:
        print("\n❌ Шаблоны не загружены!")
        print(f"   Создайте папку: {RANKS_DIR}")
        print(f"   Добавьте файлы: frame-e.png, frame-d.png, frame-c.png, ...")
    
    print("=" * 60 + "\n")