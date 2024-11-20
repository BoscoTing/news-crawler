from datetime import datetime

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_ignore_empty=True, 
        extra="ignore",
    )
    UDN_SITEMAP_URL: str
    START_DATE: datetime = datetime.strptime("2024-01-01", "%Y-%m-%d")
    END_DATE: datetime = datetime.strptime("2024-01-01", "%Y-%m-%d")

    S3_BUCKET_NAME: str
    S3_BASE_PATH: str
    S3_REGION_NAME: str

    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str

settings = Config()