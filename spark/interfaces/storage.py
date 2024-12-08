from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class IStorage(ABC):
    @abstractmethod
    def save_results(
        self,
        df: DataFrame,
        output_path: str,
        partition_cols: list | None = None,
    ) -> bool:
        """Save the processing results"""
        raise NotImplementedError
