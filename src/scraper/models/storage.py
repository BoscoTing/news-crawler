from dataclasses import dataclass


@dataclass
class ArticleData:
    url: str
    title: str
    content: str
    categories: list[str]
    keywords: list[str]
    published_at: str
    scraped_at: str
