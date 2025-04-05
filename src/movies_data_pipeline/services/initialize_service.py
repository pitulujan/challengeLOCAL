import os
from pathlib import Path
import pandas as pd

class InitializeService:
    def __init__(self):
        # Fetch paths from environment variables (required, no defaults)
        self.bronze_path = Path(os.getenv("BRONZE_BASE_PATH"))
        self.silver_base_path = Path(os.getenv("SILVER_BASE_PATH"))
        self.gold_base_path = Path(os.getenv("GOLD_BASE_PATH"))

        # Define silver and gold table schemas as class attributes
        self.silver_tables = {
            "dim_movie.parquet": ["movie_id", "name", "orig_title", "overview", "status", "crew", "genre_id", "date_id", "language_id", "country_id"],
            "dim_date.parquet": ["date_id", "date", "year", "month", "day", "quarter"],
            "dim_genre.parquet": ["genre_id", "genre_name"],
            "dim_language.parquet": ["language_id", "language_name"],
            "dim_country.parquet": ["country_id", "country_name"],
            "dim_crew.parquet": ["crew_id", "crew_name"],
            "dim_role.parquet": ["role_id", "role"],
            "bridge_movie_genre.parquet": ["movie_genre_id", "movie_id", "genre_id"],
            "bridge_movie_crew.parquet": ["movie_crew_id", "movie_id", "crew_id", "role_id", "character_name"],
            "factMoviePerformance.parquet": ["financial_id", "movie_id", "date_id", "language_id", "country_id", "score", "budget", "revenue", "profit"]
        }

        self.gold_tables = {
            "revenue_by_genre.parquet": ["genre_name", "total_revenue"],
            "avg_score_by_year.parquet": ["year", "avg_score"]
        }

    def initialize_schemas(self):
        """Initialize the data lake schemas by creating directories and empty Parquet files if they don’t exist."""
        # Create directories
        self.bronze_path.parent.mkdir(parents=True, exist_ok=True)
        self.silver_base_path.mkdir(parents=True, exist_ok=True)
        self.gold_base_path.mkdir(parents=True, exist_ok=True)

        # Bronze layer
        if not self.bronze_path.exists():
            pd.DataFrame(columns=["names", "date_x", "score", "genre", "overview", "crew", "orig_title", "status", "orig_lang", "budget_x", "revenue", "country"]).to_parquet(self.bronze_path)

        # Silver layer
        for table_name, columns in self.silver_tables.items():
            table_path = self.silver_base_path / table_name
            if not table_path.exists():
                pd.DataFrame(columns=columns).to_parquet(table_path)

        # Gold layer
        for table_name, columns in self.gold_tables.items():
            table_path = self.gold_base_path / table_name
            if not table_path.exists():
                pd.DataFrame(columns=columns).to_parquet(table_path)