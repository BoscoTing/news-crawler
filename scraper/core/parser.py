from datetime import datetime
from typing import Dict, Optional

from bs4 import BeautifulSoup

from scraper.interfaces import IParser
from scraper.models import ScrapedItem
from scraper.exceptions import ParseException


class HTMLParser(IParser):
    def __init__(self, selectors: Dict[str, str]):
        self.selectors = selectors

    def parse(self, content: str) -> Optional[ScrapedItem]:
        try:
            soup = BeautifulSoup(content, 'lxml')
            data = {}
            for key, selector in self.selectors.items():
                if key in ['keywords', 'categories']:
                    elements = soup.select(selector)
                    data[key] = [element.text.strip() for element in elements]
                else:
                    element = soup.select_one(selector)
                    data[key] = element.text.strip() if element else None
            return ScrapedItem(
                url="",  # URL will be set by the scraper
                data=data,
                scraped_at=datetime.now().isoformat()
            )
        except Exception as e:
            raise ParseException(f"Parse failed: {str(e)}")
