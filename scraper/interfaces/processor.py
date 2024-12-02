from abc import ABC, abstractmethod
from typing import List, Generic, TypeVar

from scraper.models import ProcessingResult

# Generic type for intermediate data structures
T = TypeVar('T')


class IProcessor(ABC, Generic[T]):
    """Interface for processing pipelines"""
    
    @abstractmethod
    async def process_url(self, url: str) -> ProcessingResult[T]:
        """Process a single URL through the pipeline"""
        pass
        
    @abstractmethod
    async def process_urls(self, urls: List[str]) -> List[ProcessingResult[T]]:
        """Process multiple URLs through the pipeline"""
        pass
    
    @abstractmethod
    def extract_next_level_urls(self, content: str) -> List[str]:
        """Extract URLs for the next level of processing"""
        pass
        
    @abstractmethod
    def extract_final_level_urls(self, content: str) -> List[str]:
        """Extract URLs for the final scraping target"""
        pass
