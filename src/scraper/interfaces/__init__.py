from .downloader import IDownloader
from .observer import IScraperObserver
from .parser import IParser
from .processor import IProcessor
from .storage import IStorage

__all__ = [
    "IDownloader",
    "IParser",
    "IProcessor",
    "IScraperObserver",
    "IStorage",
]