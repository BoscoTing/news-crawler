from dataclasses import dataclass


@dataclass
class ArticleData:
    url: str
    title: str
    content: str
    category: str
    published_date: str
    scraped_timestamp: str
