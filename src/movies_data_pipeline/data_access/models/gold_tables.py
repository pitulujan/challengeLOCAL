from sqlmodel import SQLModel, Field
from datetime import datetime

class revenue_by_genre(SQLModel, table=True):
    genre_name: str = Field(primary_key=True)
    total_revenue: float
    updated_at: datetime

class avg_score_by_year(SQLModel, table=True):
    year: int = Field(primary_key=True)
    avg_score: float
    updated_at: datetime