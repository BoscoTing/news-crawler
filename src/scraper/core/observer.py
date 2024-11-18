from scraper.config.logger_config import get_logger
from scraper.interfaces import IScraperObserver
from scraper.models import ScrapedItem


class LoggingObserver(IScraperObserver):
    def __init__(self, logger_name: str = "scraper.observer"):
        self.logger = get_logger(logger_name)

    async def on_item_scraped_async(self, item: ScrapedItem):
        self.logger.info(f"Successful scrape: {item.url}")

    async def on_error_async(self, error: Exception, url: str):
        self.logger.error(f"Error scraping {url}: {str(error)}")


class StatisticsObserver(IScraperObserver):
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.errors = []

    async def on_item_scraped_async(self, item: ScrapedItem):
        self.success_count += 1

    async def on_error_async(self, error: Exception, url: str):
        self.error_count += 1
        self.errors.append((url, str(error)))
