# domain/models/gold.py
from sqlmodel import SQLModel, Field, UniqueConstraint
from typing import Optional
from datetime import datetime

# Keeping integer IDs (e.g., movie_id, language_id) as primary keys and adding uniqueness constraints 
# on natural key fields (e.g., language_name, country_name) to ensure data integrity. This avoids 
# cascade updates on foreign keys, which can be a headache to manage and debug in a data pipeline.

class DimMovie(SQLModel, table=True):
    __tablename__ = "dim_movie"
    __table_args__ = (UniqueConstraint("name", "orig_title", name="uq_dim_movie"),)
    movie_id: int = Field(primary_key=True)
    name: str
    orig_title: str
    overview: str
    status: str
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class DimDate(SQLModel, table=True):
    __tablename__ = "dim_date"
    __table_args__ = (UniqueConstraint("year", "month", "day", name="uq_dim_date"),)
    date_id: int = Field(primary_key=True)
    release_date: Optional[str]
    year: int
    month: int
    day: int
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    

class DimCountry(SQLModel, table=True):
    __tablename__ = "dim_country"
    __table_args__ = (UniqueConstraint("country_name", name="uq_dim_country"),)
    country_id: int = Field(primary_key=True)
    country_name: str
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    

class DimLanguage(SQLModel, table=True):
    __tablename__ = "dim_language"
    __table_args__ = (UniqueConstraint("language_name", name="uq_dim_language"),)
    language_id: int = Field(primary_key=True)
    language_name: str
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    

class DimCrew(SQLModel, table=True):
    __tablename__ = "dim_crew"
    __table_args__ = (UniqueConstraint("actor_name", "character_name", name="uq_dim_crew"),)
    crew_id: int = Field(primary_key=True)
    actor_name: str
    character_name: str
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class DimGenre(SQLModel, table=True):
    __tablename__ = "dim_genre"
    __table_args__ = (UniqueConstraint("genre_name", name="uq_dim_genre"),)
    genre_id: int = Field(primary_key=True)
    genre_name: str
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    

class BridgeMovieGenre(SQLModel, table=True):
    __tablename__ = "bridge_movie_genre"
    __table_args__ = (UniqueConstraint("movie_id","genre_id", name="uq_bridge_movie_genre"),)
    bridge_id: int = Field(primary_key=True)
    movie_id: int = Field(foreign_key="dim_movie.movie_id")
    genre_id: int = Field(foreign_key="dim_genre.genre_id")
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class BridgeMovieCrew(SQLModel, table=True):
    __tablename__ = "bridge_movie_crew"
    __table_args__ = (UniqueConstraint("movie_id","crew_id","character_name", name="uq_bridge_movie_crew"),)
    bridge_id: int = Field(primary_key=True)
    movie_id: int = Field(foreign_key="dim_movie.movie_id")
    crew_id: int = Field(foreign_key="dim_crew.crew_id")
    character_name: str
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class FactMovieMetrics(SQLModel, table=True):
    __tablename__ = "fact_movie_metrics"
    __table_args__ = (UniqueConstraint("movie_id","date_id","country_id","language_id", name="uq_fact_movie_metrics"),)
    fact_id: int = Field(primary_key=True)
    movie_id: int = Field(foreign_key="dim_movie.movie_id")
    date_id: int = Field(foreign_key="dim_date.date_id")
    country_id: int = Field(foreign_key="dim_country.country_id")
    language_id: int = Field(foreign_key="dim_language.language_id")
    budget: float
    revenue: float
    score: float
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class LineageLog(SQLModel, table=True):
    __tablename__ = "lineage_log"
    lineage_log_id: int = Field(primary_key=True)
    lineage_id: str
    source_path: str
    stage: str
    transformation: str
    timestamp: datetime

class RevenueByGenre(SQLModel, table=True):
    __tablename__ = "revenue_by_genre"
    genre_name: str = Field(primary_key=True)
    total_revenue: float
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class AvrScoreByYear(SQLModel, table=True):
    __tablename__ = "avg_score_by_year"
    year: int = Field(primary_key=True)
    avg_score: float
    lineage_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)