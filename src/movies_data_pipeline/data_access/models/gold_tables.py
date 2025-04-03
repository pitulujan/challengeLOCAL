from sqlmodel import SQLModel, Field
from typing import Optional

class RevenueByGenre(SQLModel, table=True):
    genre_name: str = Field(primary_key=True)
    total_revenue: float

class AvgScoreByYear(SQLModel, table=True):
    year: int = Field(primary_key=True)
    avg_score: float