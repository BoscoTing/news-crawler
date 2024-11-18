from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, TypeVar, Generic

from scraper.models.scraped_item import ScrapedItem

T = TypeVar('T')


@dataclass
class ProcessingResult(Generic[T]):
    """Generic container for processing results"""
    items: List[ScrapedItem]
    metadata: Optional[T] = None


@dataclass
class SitemapMetadata:
    """Metadata specific to sitemap processing"""
    total_staticmaps: int
    total_articles: int
    categories: List[str]


@dataclass
class ArticleInfo:
    """Information about an article extracted from sitemap"""
    url: str
    published_at: datetime
