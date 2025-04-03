from typing import List, Optional
from datetime import date
from pydantic import BaseModel

class Movie(BaseModel):
    name: str
    orig_title: str
    overview: str
    status: str
    release_date: date
    genres: List[str]
    crew: List[dict]  # List of {"name": str, "role_name": str, "character_name": Optional[str]}
    country: str
    language: str
    budget: float
    revenue: float
    score: float
    is_deleted: bool = False

    def calculate_profit(self) -> float:
        """Calculate the profit of the movie."""
        return self.revenue - self.budget

    def is_profitable(self) -> bool:
        """Check if the movie made a profit."""
        return self.calculate_profit() > 0

    def mark_as_deleted(self) -> None:
        """Mark the movie as deleted (soft deletion)."""
        self.is_deleted = True

    def to_dict(self) -> dict:
        """Convert the Movie domain model to a dictionary for serialization."""
        return {
            "name": self.name,
            "orig_title": self.orig_title,
            "overview": self.overview,
            "status": self.status,
            "release_date": self.release_date.isoformat() if self.release_date else None,
            "genres": self.genres,
            "crew": self.crew,
            "country": self.country,
            "language": self.language,
            "budget": self.budget,
            "revenue": self.revenue,
            "score": self.score,
            "profit": self.calculate_profit(),
            "is_deleted": self.is_deleted
        }

    class Config:
        # Allow methods like calculate_profit to be called
        arbitrary_types_allowed = True
        # Enable serialization of date objects
        json_encoders = {
            date: lambda v: v.isoformat()
        }