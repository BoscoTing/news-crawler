from abc import ABC, abstractmethod
from typing import List, Dict

from scraper.models import ScrapedItem


class IStorage(ABC):    
    @abstractmethod
    async def save_async(self, items: List[ScrapedItem]) -> bool:
        pass
    
    @abstractmethod
    async def load_async(self, criteria: Dict) -> List[ScrapedItem]:
        """Load a list of items from storage based on criteria."""
        pass
