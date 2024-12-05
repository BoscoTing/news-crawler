import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from scraper.interfaces import IStorage
from scraper.models import ArticleData, ScrapedItem
from scraper.exceptions import StorageException


class S3Storage(IStorage):
    def __init__(
        self,
        bucket_name: str,
        base_path: str,
        region_name: str = 'ap-northeast-1',
        temp_dir: str = 'tmp'
    ):
        self.bucket = bucket_name
        self.base_path = base_path.rstrip('/')
        self.s3 = boto3.client('s3', region_name=region_name)
        
        # Set up temporary directory
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _get_partition_path(
        self,
        partition_key: datetime| None = None
    ) -> str:
        """Get S3 partition path"""
        if not partition_key:
            partition_key = datetime.now().strftime(
                'year=%Y/month=%m/initial_day=%d'
            )
        return f"{self.base_path}/{partition_key}"

    def _generate_temp_filename(self) -> Path:
        """Generate a temporary file path"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.temp_dir / f"articles_{timestamp}.parquet"

    async def save_async(self, items: List[ScrapedItem]) -> bool:
        """Save items in Parquet format with partitioning"""
        if not items:
            return False

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
                    scraped_at=item.scraped_at,
                )
                articles.append(article)

            # Convert to Arrow table
            table = pa.Table.from_pylist([vars(a) for a in articles])
            
            # Save to temporary parquet file
            temp_file = self._generate_temp_filename()
            pq.write_table(table, temp_file)
            
            # Upload to S3 with partitioning
            partition_path = self._get_partition_path(
                partition_key=articles[0].published_at.split(" ")[0]
            )
            s3_key = f"{partition_path}/articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            
            with open(temp_file, 'rb') as f:
                self.s3.upload_fileobj(f, self.bucket, s3_key)
                
            # Clean up temporary file
            temp_file.unlink()
            return True
        
        except Exception as e:
            if 'temp_file' in locals() and temp_file.exists():
                temp_file.unlink()
            raise StorageException(f"Failed to save to S3: {str(e)}")

    async def load_async(self, criteria: Dict) -> List[ScrapedItem]:
        """Load items from Parquet files in S3"""
        try:
            path = self._get_partition_path(criteria.get('partition_key'))
            
            # List objects in the partition
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=path
            )

            items = []
            for obj in response.get('Contents', []):
                if not obj['Key'].endswith('.parquet'):
                    continue

                # Download parquet file to temp location
                temp_file = self._generate_temp_filename()
                try:
                    self.s3.download_file(self.bucket, obj['Key'], str(temp_file))
                    
                    # Read parquet file
                    table = pq.read_table(temp_file)
                    df = table.to_pandas()
                    
                    # Filter based on criteria
                    for key, value in criteria.items():
                        if key != 'partition_key' and key in df.columns:
                            df = df[df[key] == value]
                    
                    # Convert to ScrapedItem objects
                    for _, row in df.iterrows():
                        item = ScrapedItem(
                            url=row['url'],
                            raw_data={
                                'title': row['title'],
                                'content': row['content'],
                                'category': row['categories'],
                                'keywords': row['keywords'],
                                'published_date': row['published_date']
                            },
                            timestamp=row['scraped_at']
                        )
                        items.append(item)
                
                finally:
                    # Clean up temp file
                    if temp_file.exists():
                        temp_file.unlink()
            
            return items

        except Exception as e:
            raise StorageException(f"Failed to load from S3: {str(e)}")

    def exists(self, key: str) -> bool:
        """Check if an object exists in S3"""
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False