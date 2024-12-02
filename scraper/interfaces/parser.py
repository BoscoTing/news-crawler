from abc import ABC, abstractmethod
from typing import Optional

from scraper.models import ScrapedItem


class IParser(ABC):
    """解析器介面"""
    @abstractmethod
    def parse(self, content: str) -> Optional[ScrapedItem]:
        pass
