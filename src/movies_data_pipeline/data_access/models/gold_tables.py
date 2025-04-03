from sqlmodel import SQLModel, Field
from datetime import datetime

class RevenueByGenre(SQLModel, table=True):
    genre_name: str = Field(primary_key=True)
    total_revenue: float
    last_updated: datetime

class AvgScoreByYear(SQLModel, table=True):
    year: int = Field(primary_key=True)
    avg_score: float
    last_updated: datetime