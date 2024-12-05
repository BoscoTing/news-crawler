import logging
import re
from datetime import datetime
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Comment, Tag

from scraper.core.scraper import Scraper
from scraper.interfaces import IProcessor
from scraper.models import (
    ArticleInfo,
    ProcessingResult,
    SitemapMetadata,
    ScrapedItem,
)


class SitemapProcessor(IProcessor[SitemapMetadata]):
    """Implementation of sitemap processing pipeline"""
    
    def __init__(
        self, 
        scraper: Scraper, 
        base_url: str = "https://money.udn.com",
        time_range: Optional[Tuple[datetime, datetime]] = None,
    ):
        self.scraper = scraper
        self.based_url = base_url
        self.time_range = time_range
        self._seen_urls = set()
        self._categories = set()
        self._staticmap_count = 0
        self._article_count = 0

    def _parse_staticmap_date(self, url: str) -> Optional[datetime]:
        """Parse the date from a staticmap URL
        Example URL format: https://money.udn.com/sitemap/staticmap/1001T202411W3
        """
        try:
            # Extract the date part (202411)
            parts = url.split('/')[-1]
            year_week = parts.split('T')[1][:6]
            year = year_week[:4]
            month = year_week[4:6]
            # Construct a datetime object for the first day of the month
            return datetime.strptime(f"{year}{month}01", "%Y%m%d")
        except (IndexError, ValueError):
            return None

    def _is_within_time_range(self, url: str) -> bool:
        """Check if the staticmap URL falls within the specified time range"""
        if not self.time_range:
            return True
            
        date = self._parse_staticmap_date(url)
        if not date:
            return True
            
        start_date, end_date = self.time_range
        return start_date <= date <= end_date
    
    def _extract_publish_date(self, url_element: Tag) -> Optional[datetime]:
        """
        Extract publish date from URL element's comments.
        
        Args:
            url_element: BeautifulSoup Tag containing the URL information
            
        Returns:
            Optional[str]: The publish date if found, None otherwise
        """
        for comment in url_element.find_all(string=lambda text: isinstance(text, Comment)):
            comment_text = comment.strip()
            # Match date pattern like <!-- 2024-11-16 18:07:03 -->
            date_match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', comment_text)
            if date_match:
                try: 
                    return datetime.strptime(
                        date_match.group(0), 
                        '%Y-%m-%d %H:%M:%S'
                    )
                except ValueError:
                    logging.error(f"Invalid date format: {date_match.group(0)}")
                    return 
        
    def extract_next_level_urls(self, content: str) -> List[str]:
        """Extract static map URLs from sitemap"""
        soup = BeautifulSoup(content, 'xml')
        staticmap_urls = []

        for staticmap in soup.find_all('sitemap'):
            loc = staticmap.find('loc')
            if loc and 'staticmap' in loc.text:
                url = loc.text.strip()
                if not url.startswith('http'):
                    url = urljoin(self.base_url, url)

                if self._is_within_time_range(url):
                    staticmap_urls.append(url)

        self._staticmap_count = len(staticmap_urls)
        return staticmap_urls
    
    def extract_final_level_urls(self, content: str) -> List[ArticleInfo]:
        """Extract article URLs from static map"""
        soup = BeautifulSoup(content, 'xml')
        article_urls = []
        
        for url_element in soup.find_all('url'):
            loc = url_element.find('loc')
            if loc:
                url = loc.text.strip()
                if not url.startswith('http'):
                    url = urljoin(self.base_url, url)
                if url in self._seen_urls:
                    continue
                self._seen_urls.add(url)

                publish_date = self._extract_publish_date(url_element)
                if url and publish_date:
                    article = ArticleInfo(
                        url=url, 
                        published_at=publish_date
                    )
                    article_urls.append(article)
                    self._article_count += 1
                
        return article_urls

    async def process_pipeline(self, sitemap_url: str) -> List[ScrapedItem]:
        """Process the complete scraping pipeline"""
        # Step 1: Scrape the sitemap
        sitemap_content = await self.scraper.downloader.download_async(sitemap_url)
        if not sitemap_content:
            return []
            
        # Step 2: Extract and process static map URLs
        staticmap_urls = self.extract_next_level_urls(sitemap_content)
        all_articles: List[ArticleInfo] = []
        
        # Step 3: Process each static map
        for staticmap_url in staticmap_urls:
            content = await self.scraper.downloader.download_async(staticmap_url)
            if content:
                articles = self.extract_final_level_urls(content)
                all_articles.extend(articles)
        
        # Step 4: Scrape all article content
        article_urls = [article.url for article in all_articles]
        scraped_items = await self.scraper.scrape_urls_async(article_urls)

        if scraped_items:
            await self.scraper.storage.save_async(scraped_items)
        
        return scraped_items
