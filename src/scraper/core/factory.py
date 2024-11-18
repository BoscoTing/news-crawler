from pathlib import Path
from typing import Dict, Optional

from scraper.interfaces import IDownloader, IParser, IStorage
from scraper.core.downloader import Downloader
from scraper.core.parser import HTMLParser
from scraper.core.storages import JSONStorage, S3Storage


class ScraperFactory:
    @staticmethod
    def create_downloader(
        retry_times: int = 3,
        timeout: int = 10
    ) -> IDownloader:
        return Downloader(retry_times, timeout)

    @staticmethod
    def create_parser(selectors: Dict[str, str]) -> IParser:
        return HTMLParser(selectors)

    @staticmethod
    def create_storage(
        storage_type: str = 'json',
        file_path: Optional[str] = None,
        s3_config: Optional[dict] = None,
    ) -> IStorage:
        """
        Create storage based on type
        
        Args:
            storage_type: "json", "s3-parquet"
            file_path: Path for JSON storage
            s3_config: Configuration for S3 storage containing:
                      - bucket_name
                      - base_path
                      - region_name (optional)
        """
        if storage_type == "json":
            if not file_path:
                file_path = "data/scraped_data.json"
            return JSONStorage(Path(file_path))
        
        elif storage_type == "s3-parquet":
            if not s3_config:
                raise ValueError("S3 configuration required for s3-spark storage")
            return S3Storage(
                bucket_name=s3_config["bucket_name"],
                base_path=s3_config["base_path"],
                region_name=s3_config.get("region_name", "ap-northeast-1")
            )
        
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
