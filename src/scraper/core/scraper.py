from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

import asyncio

from scraper.interfaces import (
    IDownloader,
    IParser,
    IStorage,
    IScraperObserver,
)
from scraper.models import ScrapedItem


class Scraper:
    def __init__(
        self,
        downloader: IDownloader,
        parser: IParser,
        storage: Optional[IStorage] = None,
        max_concurrent: int = 3
    ):
        self.downloader = downloader
        self.parser = parser
        self.storage = storage
        self.max_concurrent = max_concurrent
        self.observers: List[IScraperObserver] = []
        self.semaphore = asyncio.Semaphore(max_concurrent)

    def add_observer(self, observer: IScraperObserver):
        self.observers.append(observer)

    async def _notify_item_scraped_async(self, item: ScrapedItem):
        """Notify observers the item has been scraped"""
        for observer in self.observers:
            await observer.on_item_scraped_async(item)

    async def _notify_error_async(self, error: Exception, url: str):
        for observer in self.observers:
            await observer.on_scraped_error_async(error, url)

    @asynccontextmanager
    async def _limited_concurrency(self):
        """A context manager to limit concurrency"""
        await self.semaphore.acquire() # Block if the maximum concurrency is reached
        try:
            yield
        finally:
            self.semaphore.release()

    async def scrape_url_async(self, url: str) -> Optional[ScrapedItem]:
        """Scrape a single url"""
        async with self._limited_concurrency():
            try:
                content = await self.downloader.download_async(url)
                if content:
                    item = self.parser.parse(content)
                    if item:
                        item.url = url
                        item.scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        await self._notify_item_scraped_async(item)
                        return item
            except Exception as e:
                await self._notify_error_async(e, url)
            return None

    async def scrape_urls_async(self, urls: List[str]) -> List[ScrapedItem]:
        """Scrape multiple urls asynchronously"""
        urls = urls[0:2]
        tasks = [self.scrape_url_async(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        scraped_items = [
            item for item in results 
            if isinstance(item, ScrapedItem)
        ]
        print(scraped_items)
        if self.storage and scraped_items:
            await self.storage.save_async(scraped_items)
            
        return scraped_items

    @classmethod
    async def create(
        cls, 
        downloader: IDownloader, 
        parser: IParser,            
        storage: Optional[IStorage] = None,
        max_concurrent: int = 3
    ) -> 'Scraper':
        """Use a factory method to create a scraper instance"""
        return cls(
            downloader, 
            parser, 
            storage, 
            max_concurrent
        )
