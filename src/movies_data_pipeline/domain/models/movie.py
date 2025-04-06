from typing import List, Optional, Dict
from datetime import date
from pydantic import BaseModel

class Movie(BaseModel):
    name: str
    orig_title: str
    overview: str
    status: str
    release_date: Optional[date] = None
    genres: List[str] = []
    crew: List[Dict[str, str]] = []
    language: Optional[str] = None


def to_dict(self) -> dict:
    return {
        "name": self.name,
        "orig_title": self.orig_title,
        "overview": self.overview,
        "status": self.status,
        "release_date": self.release_date.isoformat() if self.release_date else None,
        "genres": self.genres,
        "crew": self.crew,
        "language": self.language,
    }

class Config:
    arbitrary_types_allowed = True
    json_encoders = {
        date: lambda v: v.isoformat()
    }