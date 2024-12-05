from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


@dataclass
class ScrapedItem:
    url: str
    data: Any
    scraped_at: str
