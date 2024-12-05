import json
from pathlib import Path
from typing import List, Dict

import aiofiles

from scraper.interfaces import IStorage
from scraper.models import ScrapedItem


class JSONStorage(IStorage):
    """Store data in JSON file"""
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, items: List[ScrapedItem]) -> bool:
        try:
            data = [vars(item) for item in items]
            with self.file_path.open('w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            raise StorageException(f"Save failed: {str(e)}")
        
    async def save_async(self, items: List[ScrapedItem]) -> bool:
        try:
            data = [vars(item) for item in items]
            async with aiofiles.open(self.file_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            return True
        except Exception as e:
            raise StorageException(f"Save failed: {str(e)}")

    def load(self, criteria: Dict) -> List[ScrapedItem]:
        try:
            if not self.file_path.exists():
                return []
                
            with self.file_path.open('r', encoding='utf-8') as f:
                data = json.load(f)
                
            items = []
            for item_data in data:
                if all(item_data.get(k) == v for k, v in criteria.items()):
                    items.append(ScrapedItem(**item_data))
            return items
        except Exception as e:
            raise StorageException(f"Load failed: {str(e)}")
        
    async def load_async(self, criteria: Dict) -> List[ScrapedItem]:
        try:
            if not self.file_path.exists():
                return []
                
            async with aiofiles.open(self.file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                data = json.loads(content)
                
            items = []
            for item_data in data:
                if all(item_data.get(k) == v for k, v in criteria.items()):
                    items.append(ScrapedItem(**item_data))
            return items
        except Exception as e:
            raise StorageException(f"Load failed: {str(e)}")
        

class StorageException(Exception):
    pass
