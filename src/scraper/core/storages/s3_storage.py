from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from scraper.interfaces import IStorage 
from scraper.exceptions import StorageException
from scraper.models import ArticleData, ScrapedItem

    
class S3Storage(IStorage):
    def __init__(
        self,
        bucket_name: str,
        base_path: str,
        temp_dir: str = 'data/tmp',
        region_name: str = 'ap-northeast-1',
    ):
        self.bucket = bucket_name
        self.base_path = base_path.rstrip('/')
        self.s3 = boto3.client('s3', region_name=region_name)

        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _generate_temp_filename(self) -> Path:
        """Generate a temporary file path"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.temp_dir / f"articles_{timestamp}.parquet"

    def _get_partition_path(self, dt: Optional[datetime] = None) -> str:
        """Creates partition path using year/month/day structure"""
        if not dt:
            dt = datetime.now()
        return f"{self.base_path}/year={dt.year}/month={dt.month:02d}/initial_day={dt.day:02d}"

    async def save_async(self, items: List[ScrapedItem]) -> bool:
        """Save items in Parquet format with partitioning"""
        try:
            # Transform scraped items to structured format
            articles = []
            for item in items:
                article = ArticleData(
                    url=item.url,
                    title=item.data.get('title', ''),
                    content=item.data.get('content', ''),
                    categories=item.data.get('category', ''),
                    keywords=item.data.get('keywords', ''),
                    published_at=item.data.get('published_at', ''),
                    scraped_at=item.scraped_at
                )
                articles.append(article)

            # Convert to Arrow table
            table = pa.Table.from_pylist([vars(a) for a in articles])
            
            # Save to temporary parquet file
            temp_file = self._generate_temp_filename()
            pq.write_table(table, temp_file)
            
            # Upload to S3 with partitioning
            published_dt = datetime.strptime(articles[0].published_at, "%Y/%m/%d %H:%M:%S")
            partition_path = self._get_partition_path(dt=published_dt)
            s3_key = f"{partition_path}/articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            
            with open(temp_file, 'rb') as f:
                self.s3.upload_fileobj(f, self.bucket, s3_key)
            # Clean up temporary file
            temp_file.unlink()

            return True
            
        except Exception as e:
            raise StorageException(f"Failed to save parquet to S3: {str(e)}")

    async def load_async(self, criteria: Dict) -> List[ScrapedItem]:
        """Load items from S3 based on criteria"""
        try:
            # TODO: Implement loading logic
            return []
        except Exception as e:
            raise StorageException(f"Failed to load parquet from S3: {str(e)}")
