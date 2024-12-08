from typing import Optional
from urllib.parse import quote_plus

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType

from db.config import settings
from spark.interfaces.storage import IStorage


class PostgreSQLStorage(IStorage):
    """Implementation of Spark storage for PostgreSQL database"""
    
    def __init__(self):
        # Construct JDBC URL with properly escaped credentials
        user = quote_plus(settings.POSTGRES_USER)
        password = quote_plus(settings.POSTGRES_PASSWORD)
        host = settings.POSTGRES_SERVER
        port = settings.POSTGRES_PORT
        db = settings.POSTGRES_DB
        
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }

    def save_results(
        self,
        df: DataFrame,
        output_path: str,
        partition_cols: Optional[list] = None,
    ) -> bool:
        """Save sentiment analysis results to PostgreSQL
        
        Args:
            df: Spark DataFrame containing sentiment analysis results
            output_path: Target table name
            partition_cols: Not used for PostgreSQL storage
            
        Returns:
            bool: True if save was successful
        """
        try:
            # Prepare DataFrame for database insertion
            prepared_df = (
                df.select(
                    "title",
                    "url",
                    F.col("published_at").cast(TimestampType()),
                    F.col("sentiment_score").cast("decimal(5, 4)"),
                    "sentiment_label",
                    F.col("analyzed_at").cast(TimestampType()),
                    F.current_timestamp().alias("created_at"),
                )
            )

            prepared_df.show()

            # Write to PostgreSQL
            prepared_df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", output_path) \
                .option("user", self.connection_properties["user"]) \
                .option("password", self.connection_properties["password"]) \
                .option("driver", self.connection_properties["driver"]) \
                .mode("append") \
                .save()

            return True

        except Exception as e:
            raise StorageError(f"Failed to save to PostgreSQL: {str(e)}")


class StorageError(Exception):
    """Custom exception for storage errors"""
    pass
