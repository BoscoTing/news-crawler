from dataclasses import dataclass
from typing import Any


@dataclass
class ScrapedItem:
    url: str
    data: Any
    scraped_at: str
