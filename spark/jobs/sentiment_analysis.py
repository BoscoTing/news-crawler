import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

from spark.common.spark_session import create_spark_session
from spark.core import (
    ChineseFinBERTProcessor,
    PostgreSQLStorage,
    SparkParquetReader,
)
from spark.config.spark_config import settings


def run_sentiment_analysis(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    """Run sentiment analysis on news articles"""

    reader = SparkParquetReader(
        spark=spark,
        s3_bucket=settings.S3_BUCKET_NAME,
        s3_access_key=settings.AWS_ACCESS_KEY_ID,
        s3_secret_key=settings.AWS_SECRET_ACCESS_KEY,
    )

    df = reader.read_parquet(
        base_path=input_path,
        start_date=start_date,
        end_date=end_date,
    )

    processor = ChineseFinBERTProcessor(
        spark=spark,
        model_name=settings.FIN_BERT_MODEL_PATH,
    )

    df = processor.preprocess_dataframe(df)
    sentiment_df = processor.analyze_sentiment(df)
    aggregated_df = processor.get_aggregated_sentiment(sentiment_df)

    storage = PostgreSQLStorage()
    
    storage.save_results(
        df=aggregated_df,
        output_path=output_path,
        partition_cols=["year", "month", "day"],
    )

    return df


if __name__ == "__main__":
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=30)

    end_date = datetime.strptime("2024-02-04", "%Y-%m-%d")
    start_date = end_date
    
    # Initialize Spark session
    spark = create_spark_session(
        app_name="NewsArticleSentimentAnalysis",
        s3_access_key=settings.AWS_ACCESS_KEY_ID,
        s3_secret_key=settings.AWS_SECRET_ACCESS_KEY,
    )
    
    try:
        run_sentiment_analysis(
            spark=spark,
            input_path="/raw/news",
            output_path="article_sentiment",
            start_date=start_date,
            end_date=end_date
        )
    finally:
        spark.stop()
