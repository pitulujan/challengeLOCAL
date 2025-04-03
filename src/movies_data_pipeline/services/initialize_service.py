import pandas as pd
import os
from movies_data_pipeline.data_access.database import get_session_direct

def initialize_schemas():
    # Define base directories
    bronze_dir = "/app/movies_data_pipeline/data_access/data_lake/bronze/"
    silver_dir = "/app/movies_data_pipeline/data_access/data_lake/silver/"
    gold_dir = "/app/movies_data_pipeline/data_access/data_lake/gold/"

    # Create directories if they don't exist
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)

    # Bronze
    bronze_path = os.path.join(bronze_dir, "movies.parquet")
    if not os.path.exists(bronze_path):
        pd.DataFrame(columns=["names", "date_x", "score", "genre", "overview", "crew", "orig_title", "status", "orig_lang", "budget_x", "revenue", "country"]).to_parquet(bronze_path)

    # Silver
    silver_tables = {
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

    for table_name, columns in silver_tables.items():
        table_path = os.path.join(silver_dir, table_name)
        if not os.path.exists(table_path):
            pd.DataFrame(columns=columns).to_parquet(table_path)

    # Gold
    gold_tables = {
        "revenue_by_genre.parquet": ["genre_name", "total_revenue"],
        "avg_score_by_year.parquet": ["year", "avg_score"]
    }

    for table_name, columns in gold_tables.items():
        table_path = os.path.join(gold_dir, table_name)
        if not os.path.exists(table_path):
            pd.DataFrame(columns=columns).to_parquet(table_path)