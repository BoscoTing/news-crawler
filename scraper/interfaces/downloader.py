from typing import Optional
from abc import ABC, abstractmethod


class IDownloader(ABC):
    @abstractmethod
    async def download_async(self, url: str) -> Optional[str]:
        """Download content asynchronously from a URL."""
        pass

    @abstractmethod
    async def close(self):
        """Close any resources or sessions."""
        pass

    @abstractmethod
    async def __aenter__(self):
        """Enter the async context."""
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context and handle any exception."""
        pass
