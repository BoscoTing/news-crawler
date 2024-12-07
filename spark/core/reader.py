from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

from spark.common.spark_session import create_spark_session
from spark.interfaces import ISparkReader


class SparkParquetReader(ISparkReader):
    def __init__(
        self,
        spark: SparkSession,
        s3_bucket: str = "udn-news",
        s3_access_key: str | None = None,
        s3_secret_key: str | None = None,
        s3_endpoint: str | None = None,
        partition_cols: list[str] | None = None,
    ):
        """Initialize the Spark Parquet reader

        Args:
            app_name: Name of the Spark application
            s3_bucket: S3 bucket name
            s3_access_key: AWS access key ID
            s3_secret_key: AWS secret access key
            s3_endpoint: Custom S3 endpoint (for MinIO etc.)
            partition_cols: List of partition columns, defaults to ["year", "month", "day"]
        """
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint = s3_endpoint
        self.partition_cols = partition_cols or ["year", "month", "day"]


    def _get_s3_path(
        self,
        base_path: str,
        partition_values: dict[str, str] | None = None,
    ) -> str:
        """Construct S3 path with optional partitioning

        Args:
            base_path: Base path in the S3 bucket
            partition_values: Dictionary of partition column names and values

        Returns:
            Complete S3 path including bucket, base path and partitions
        """
        s3_path = f"s3a://{self.s3_bucket}/{base_path}"
        if partition_values:
            partition_path = "/".join(
                f"{key}={value}" for key, value in partition_values.items()
            )
            s3_path = f"{s3_path}/{partition_path}"
        print(s3_path)
        return s3_path
    
    def read_parquet(
        self,
        base_path: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        columns: list[str] | None = None,
    ) -> DataFrame:
        """Read Parquet files from S3 with optional date filtering

        Args:
            base_path: Base path in the S3 bucket
            start_date: Start date for filtering
            end_date: End date for filtering
            columns: List of columns to select

        Returns:
            Spark DataFrame containing the read data
        """
        if start_date and end_date:
            partition_filters = []
            interval = (end_date - start_date).days + 1
            for current_date in (start_date + timedelta(n) for n in range(interval)):
                partition_values = {
                    "year": current_date.strftime("%Y"),
                    "month": current_date.strftime("%m"),
                    "weekstart": current_date.strftime("%d"),
                }
                partition_filters.append(
                    self._get_s3_path(base_path, partition_values)
                )
            try:
                df = self.spark.read.parquet(*partition_filters)
            except Exception as e:
                raise ValueError(f"Failed to read Parquet files: {str(e)}")
        else:
            try:
                df = self.spark.read.parquet(self._get_s3_path(base_path))
            except Exception as e:
                raise ValueError(f"Failed to read Parquet file: {str(e)}")
        
        if columns:
            df = df.select(columns)
        return df

    def close(self):
        """Stop the Spark session and clean up resources"""
        if self.spark:
            self.spark.stop()
            self.spark = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
