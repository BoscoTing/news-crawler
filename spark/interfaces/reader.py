from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.sql import DataFrame


class ISparkReader(ABC):

    @abstractmethod
    def read_parquet(
        self,
        base_path: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        columns: list[str] | None = None,
    ) -> DataFrame:
        """Read Parquet file from S3 with optional filtering"""
        raise NotImplementedError
    
    @abstractmethod
    def close(self):
        """Clean up resources"""
        raise NotImplementedError
