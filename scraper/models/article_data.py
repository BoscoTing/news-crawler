from dataclasses import dataclass
from datetime import datetime


@dataclass
class ArticleData:
    url: str
    title: str
    content: str
    categories: list[str]
    keywords: list[str]
    published_at: str
    scraped_at: str
