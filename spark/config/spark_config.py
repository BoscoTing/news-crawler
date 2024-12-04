from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class SparkConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_ignore_empty=True, 
        extra="ignore",
    )
    ENVIRONMENT: Literal[
        "local", 
        "staging", 
        "production"
    ] = "local"

    S3_BUCKET_NAME: str
    S3_BASE_PATH: str
    S3_REGION_NAME: str
    S3_ENDPOINT: str | None = None

    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str

    FIN_BERT_MODEL_PATH: str

settings = SparkConfig()
