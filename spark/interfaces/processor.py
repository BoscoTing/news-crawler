from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class ISparkProcessor(ABC):

    @abstractmethod
    def preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        """Preprocess the input dataframe before analysis"""
        raise NotImplementedError


class ISparkSentimentProcessor(ISparkProcessor):

    @abstractmethod
    def analyze_sentiment(self, df: DataFrame) -> DataFrame:
        """Perform sentiment analysis on the preprocessed data"""
        raise NotImplementedError
    
    @abstractmethod
    def get_aggregated_sentiment(self, df: DataFrame) -> DataFrame:
        """Get aggregated sentiment scores from the analyzed data"""
        raise NotImplementedError
