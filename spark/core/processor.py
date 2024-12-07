import pandas as pd
import torch
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf, col, current_timestamp
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    StringType,
    StructType,
    StructField,
    TimestampType,
)
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from transformers import pipeline
from typing import Iterator

from spark.interfaces import ISparkSentimentProcessor
from spark.config.spark_config import settings


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
        )

    def preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        cleaned_df = df.selectExpr(
            "*",
            "concat(title, ' ', content) as text",
        )

        return cleaned_df
    
    def analyze_sentiment(self, df: DataFrame) -> DataFrame:
        """Creates sentiment analysis UDF using pipeline"""
        analysis_pipeline = self._create_pipeline()

        # Mapping for sentiment labels
        LABEL_MAP = {
            'LABEL_0': 'positive',
            'LABEL_1': 'neutral',
            'LABEL_2': 'negative'
        }

        @pandas_udf(StructType([
            StructField("score", FloatType(), True),
            StructField("sentiment", StringType(), True)
        ]))
        def _analyze_sentiment(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
            for batch in batch_iter:
                if len(batch) == 0:
                    yield pd.DataFrame(columns=["score", "sentiment"])
                    continue
                
                results = analysis_pipeline(batch.to_list())
                batch_results = pd.DataFrame({
                    "score": [result['score'] for result in results],
                    "sentiment": [LABEL_MAP[result['label']] for result in results]
                })
                
                yield batch_results

        return df.selectExpr(
            "*",
            "sentiment_result.score as sentiment_score",
            "sentiment_result.sentiment as sentiment_label"
        ).withColumn("sentiment_result", _analyze_sentiment("text"))

    def get_aggregated_sentiment(self, df: DataFrame) -> DataFrame:
        return df.select(
            "*",
            current_timestamp().alias("analyzed_at")
        ).drop("sentiment_result", "text", "content")
