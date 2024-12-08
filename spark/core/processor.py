import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf, current_timestamp, col
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StructType,
    StructField,
)
from transformers import pipeline
from typing import Iterator

from spark.interfaces import ISparkSentimentProcessor


class ChineseFinBERTProcessor(ISparkSentimentProcessor):
    def __init__(
        self,
        spark: SparkSession,
        model_name: str | None = None,
        max_length: int = 512,
        batch_size: int = 32,
    ):
        self.spark = spark
        self.model_name = model_name
        self.pipeline = None
        self.model = None
        self.max_length = max_length
        self.batch_size = batch_size
        self.metrics = {}
        self._create_pipeline()

    def _create_pipeline(self) -> None:
        return pipeline(
            task="sentiment-analysis", 
            model=self.model_name,
            max_length=self.max_length,
            batch_size=self.batch_size,
            truncation=True,
        )

    def preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df \
            .dropDuplicates(["title", "url"]) \
            .selectExpr(
                "*",
                "concat(title, ' ', content) as text",
            )
    
    def analyze_sentiment(self, df: DataFrame) -> DataFrame:
        """Creates sentiment analysis UDF using pipeline"""
        analysis_pipeline = self._create_pipeline()

        @pandas_udf(StructType([
            StructField("score", FloatType(), True),
            StructField("label", IntegerType(), True)
        ]))
        def _analyze_sentiment(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
            for batch in batch_iter:
                if len(batch) == 0:
                    yield pd.DataFrame(columns=["score", "label"])
                    continue
                
                results = analysis_pipeline(batch.to_list())
                batch_results = pd.DataFrame({
                    "score": [result['score'] for result in results],
                    "label": [int(result['label'].split("_")[-1]) for result in results],
                })
                
                yield batch_results

        return df.select(
            "*",
            _analyze_sentiment("text").alias("sentiment_result")
        ).select("*",
                 col("sentiment_result.score").alias("sentiment_score"),
                 col("sentiment_result.label").alias("sentiment_label"))

    def get_aggregated_sentiment(self, df: DataFrame) -> DataFrame:
        return df.select(
            "*",
            current_timestamp().alias("analyzed_at")
        ).drop("sentiment_result", "text", "content")
