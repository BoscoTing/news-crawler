from datetime import datetime
from zoneinfo import ZoneInfo

from sqlmodel import (
    Column, 
    Field,
    SQLModel,
)
from sqlalchemy import (
    BigInteger,
    DateTime,
    Numeric,
    VARCHAR, 
)

db_timezone = ZoneInfo('UTC')


class ArticleSentiment(SQLModel, table=True):
    id: int | None = Field(
        sa_column=Column(BigInteger,
                         autoincrement=True,
                         primary_key=True)
    )
    title: str = Field(
        min_length=1,
        max_length=60,
        sa_column=Column(VARCHAR),
    )
    url: str = Field(max_length=60, unique=True)
    published_at: datetime = Field(
        sa_column=Column(DateTime(timezone=True),
                         index=True),
    )
    sentiment_score: float = Field(
        sa_column=Column(Numeric(precision=3))
    )
    sentiment_label: str = Field(
        min_length=1,
        max_length=8,
        sa_column=Column(VARCHAR),
    )
    analyzed_at: datetime = Field(
        sa_column=Column(DateTime(timezone=True)),
    )
    created_at: datetime = Field(
        default=datetime.now(db_timezone),
        sa_column=Column(DateTime(timezone=True)),
    )
