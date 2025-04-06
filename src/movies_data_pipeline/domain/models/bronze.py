from typing import List, Optional
from datetime import date
from pydantic import BaseModel

class BronzeMovieUpdate(BaseModel):
    uuid: str  # Required field for bronze updates, this give as flexibility to update just one or a fez records without the need of all the keys 
    name: Optional[str] = None
    orig_title: Optional[str] = None
    overview: Optional[str] = None
    status: Optional[str] = None
    release_date: Optional[date] = None
    genres: Optional[List[str]] = None
    crew: Optional[List[dict]] = None
    country: Optional[str] = None
    language: Optional[str] = None
    budget: Optional[float] = None
    revenue: Optional[float] = None
    score: Optional[float] = None
    is_deleted: Optional[bool] = None

    class Config:
        json_encoders = {
            date: lambda v: v.isoformat() if v else None
        }