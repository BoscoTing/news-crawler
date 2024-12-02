from abc import ABC, abstractmethod
from scraper.models.scraped_item import ScrapedItem


class IScraperObserver(ABC):
    """爬蟲觀察者介面"""
    @abstractmethod
    def on_item_scraped_async(self, item: ScrapedItem):
        pass
    
    @abstractmethod
    def on_scraped_error_async(self, error: Exception, url: str):
        pass
