import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import logging
from pathlib import Path
import os
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.data_access.database import get_session_direct 
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, bronze_file_path: str, db_session: Session = None):
        """Initialize the Transformer with bronze file path."""
        self.bronze_file_path = Path(bronze_file_path)
        self.silver_file_path = Path(os.getenv("SILVER_BASE_PATH")) / "silver_movies.parquet"
        self.db_session = db_session  


    def transform(self) -> Dict[str, pd.DataFrame]:
        logger.info("Starting transformation")
        try:
            if not self.bronze_file_path.exists():
                logger.info(f"Bronze file {self.bronze_file_path} does not exist; returning empty result")
                return {}
            bronze_df = pd.read_parquet(self.bronze_file_path)
        except Exception as e:
            logger.error(f"Failed to load bronze data: {str(e)}")
            raise

        if bronze_df.empty:
            logger.info("No new data to transform; returning empty result")
            return {}

        # Create silver layer
        silver_df, lineage_entries = self._create_silver_layer(bronze_df)
        
        # Save silver layer
        silver_df.to_parquet(self.silver_file_path, index=False)
        
        # Create gold tables
        gold_tables = self._create_gold_tables(silver_df, lineage_entries)
      
        logger.info("Transformation completed successfully")
        return gold_tables

    def _create_silver_layer(self, df: pd.DataFrame) -> tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Create silver layer by cleaning and standardizing bronze data with lineage tracking."""
        df = self._standardize_columns(df)
        if "bronze_id" not in df.columns:
            raise KeyError("Input bronze data must contain a 'bronze_id' column.")
        
        lineage_entries = []
        lineage_log_id_counter = 1
        
        # Initial load from bronze
        for _, row in df.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["bronze_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "bronze",
                "transformation": "loaded",
                "timestamp": datetime.now()
            })
            lineage_log_id_counter += 1
        
        # Process dates
        df = self._process_dates(df)
        for _, row in df.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["bronze_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "silver",
                "transformation": "dates_processed",
                "timestamp": datetime.now()
            })
            lineage_log_id_counter += 1
        
        # Process genre and crew
        df = self._process_genre_and_crew(df)
        for _, row in df.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["bronze_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "silver",
                "transformation": "genre_crew_processed",
                "timestamp": datetime.now()
            })
            lineage_log_id_counter += 1
        
        # Finalize silver layer
        current_time = datetime.now()
        df["created_at"] = current_time
        df["updated_at"] = current_time
        df["silver_id"] = range(1, len(df) + 1)  # Assign silver_id for later use
        
        for _, row in df.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["bronze_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "silver",
                "transformation": "finalized",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        return df, lineage_entries

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and ensure 'name' and 'orig_title' are not null."""

        if "names" in df.columns:
            if "name" not in df.columns:
                df["name"] = df["names"]
            else:
                df["name"] = df["name"].fillna(df["names"])
            df = df.drop(columns=["names"])
        
        if "name" not in df.columns:
            raise KeyError("Input data must contain a 'name' column for movie titles.")
        
        if "orig_title" not in df.columns:
            logger.warning("Input data does not contain 'orig_title'; filling with 'name'")
            df["orig_title"] = df["name"]
        
        df["name"] = df["name"].fillna(df["orig_title"])
        df["orig_title"] = df["orig_title"].fillna(df["name"])
        df["name"] = df["name"].fillna("Unknown")
        df["orig_title"] = df["orig_title"].fillna("Unknown")
        
        return df

    def _process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and standardize date columns."""
        possible_date_cols = ["date_x", "release_date", "date"]
        date_col = next((col for col in possible_date_cols if col in df.columns), None)
        
        if date_col:
            df = df.rename(columns={date_col: "date_x"})
            df["date_x"] = df["date_x"].astype(str).str.strip()
            df["date_x"] = pd.to_datetime(df["date_x"], format="%m/%d/%Y", errors="coerce")
        else:
            raise KeyError("Input data must contain a date column ('date_x', 'release_date', or 'date')")
        
        return df

    def _process_genre_and_crew(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process genre and crew data into structured formats."""
        df["genre"] = df["genre"].fillna("Unknown").replace("", "Unknown")
        df["genre_list"] = df["genre"].str.split(",\s+")
        df["crew_pairs"] = df["crew"].apply(self._parse_crew)
        return df

    def _parse_crew(self, crew_str: str) -> List[Dict[str, str]]:
        """Parse crew string into a list of dictionaries."""
        if pd.isna(crew_str) or crew_str == "":
            return []
        
        crew_list = crew_str.split(", ")
        pairs = []
        for i in range(0, len(crew_list), 2):
            if i + 1 < len(crew_list):
                pairs.append({"actor_name": crew_list[i], "character_name": crew_list[i + 1]})
            else:
                pairs.append({"actor_name": crew_list[i], "character_name": "Self"})
        return pairs

    def _create_gold_tables(self, silver_df: pd.DataFrame, lineage_entries: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """Create gold layer tables with unique constraints."""
        current_time = datetime.now()
        lineage_log_id_counter = max(entry["lineage_log_id"] for entry in lineage_entries) + 1 if lineage_entries else 1
        
        # Create dim_movie with silver_id
        dim_movie = silver_df[["name", "orig_title", "overview", "status", "silver_id"]].drop_duplicates(subset=["name", "orig_title"])
        # Rename silver_id to lineage_id
        dim_movie = dim_movie.rename(columns={"silver_id": "lineage_id"})
        # Add movie_id and timestamps
        dim_movie["movie_id"] = range(1, len(dim_movie) + 1)
        dim_movie["created_at"] = current_time
        dim_movie["updated_at"] = current_time
        # Reorder columns to match the database schema
        dim_movie = dim_movie[["movie_id", "name", "orig_title", "overview", "status", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_movie.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["movie_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_movie",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Assign movie_id to silver_df
        silver_df = silver_df.merge(dim_movie[["name", "orig_title", "movie_id"]], on=["name", "orig_title"], how="left")
        
        # Create dim_date
        dim_date = silver_df[["date_x"]].drop_duplicates()
        dim_date["release_date"] = pd.to_datetime(dim_date["date_x"])
        dim_date["year"] = dim_date["release_date"].dt.year
        dim_date["month"] = dim_date["release_date"].dt.month
        dim_date["day"] = dim_date["release_date"].dt.day
        dim_date = dim_date.drop_duplicates(subset=["year", "month", "day"])
        dim_date["date_id"] = range(1, len(dim_date) + 1)
        dim_date["lineage_id"] = range(1, len(dim_date) + 1)  # Add lineage_id
        dim_date = dim_date.drop(columns=["date_x"])
        dim_date["created_at"] = current_time
        dim_date["updated_at"] = current_time
        # Reorder columns to include lineage_id
        dim_date = dim_date[["date_id", "release_date", "year", "month", "day", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_date.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["date_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_date",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Dimension: dim_country
        dim_country = silver_df[["country"]].drop_duplicates()
        dim_country = dim_country.rename(columns={"country": "country_name"})
        dim_country["country_id"] = range(1, len(dim_country) + 1)
        dim_country["lineage_id"] = range(1, len(dim_country) + 1)  # Add lineage_id
        dim_country["created_at"] = current_time
        dim_country["updated_at"] = current_time
        dim_country = dim_country[["country_id", "country_name", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_country.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["country_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_country",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Dimension: dim_language
        dim_language = silver_df[["orig_lang"]].drop_duplicates()
        dim_language = dim_language.rename(columns={"orig_lang": "language_name"})
        dim_language["language_id"] = range(1, len(dim_language) + 1)
        dim_language["lineage_id"] = range(1, len(dim_language) + 1)  # Add lineage_id
        dim_language["created_at"] = current_time
        dim_language["updated_at"] = current_time
        dim_language = dim_language[["language_id", "language_name", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_language.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["language_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_language",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Dimension: dim_crew
        crew_flat = silver_df.explode("crew_pairs")[["crew_pairs"]].dropna()
        dim_crew = pd.DataFrame(crew_flat["crew_pairs"].tolist())
        dim_crew = dim_crew.drop_duplicates(subset=["actor_name", "character_name"])
        dim_crew["crew_id"] = range(1, len(dim_crew) + 1)
        dim_crew["lineage_id"] = range(1, len(dim_crew) + 1)  # Add lineage_id
        dim_crew["created_at"] = current_time
        dim_crew["updated_at"] = current_time
        dim_crew = dim_crew[["crew_id", "actor_name", "character_name", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_crew.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["crew_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_crew",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Dimension: dim_genre
        dim_genre = silver_df.explode("genre_list")[["genre_list"]].drop_duplicates()
        dim_genre = dim_genre.rename(columns={"genre_list": "genre_name"})
        dim_genre["genre_id"] = range(1, len(dim_genre) + 1)
        dim_genre["lineage_id"] = range(1, len(dim_genre) + 1)  # Add lineage_id
        dim_genre["created_at"] = current_time
        dim_genre["updated_at"] = current_time
        dim_genre = dim_genre[["genre_id", "genre_name", "created_at", "lineage_id", "updated_at"]]
        
        for _, row in dim_genre.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["genre_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_genre",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Bridge: bridge_movie_genre
        bridge_movie_genre = silver_df.explode("genre_list")[["movie_id", "genre_list"]]
        bridge_movie_genre = bridge_movie_genre.merge(dim_genre[["genre_id", "genre_name"]], left_on="genre_list", right_on="genre_name", how="left")
        bridge_movie_genre = bridge_movie_genre.drop_duplicates(subset=["movie_id", "genre_id"])
        bridge_movie_genre["bridge_id"] = range(1, len(bridge_movie_genre) + 1)
        bridge_movie_genre["lineage_id"] = range(1, len(bridge_movie_genre) + 1)  # Add lineage_id
        bridge_movie_genre["created_at"] = current_time
        bridge_movie_genre["updated_at"] = current_time
        bridge_movie_genre = bridge_movie_genre[["bridge_id", "movie_id", "genre_id", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in bridge_movie_genre.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["bridge_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_bridge_movie_genre",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Bridge: bridge_movie_crew
        bridge_movie_crew = silver_df.explode("crew_pairs")[["movie_id", "crew_pairs"]]
        bridge_movie_crew = bridge_movie_crew.dropna(subset=["crew_pairs"])
        bridge_movie_crew = bridge_movie_crew.reset_index(drop=True)
        crew_expanded = pd.DataFrame(bridge_movie_crew["crew_pairs"].tolist())
        bridge_movie_crew = pd.concat([bridge_movie_crew[["movie_id"]], crew_expanded], axis=1)
        bridge_movie_crew = bridge_movie_crew.merge(dim_crew[["crew_id", "actor_name", "character_name"]], on=["actor_name", "character_name"], how="left")
        bridge_movie_crew = bridge_movie_crew.drop_duplicates(subset=["movie_id", "crew_id", "character_name"])
        bridge_movie_crew["bridge_id"] = range(1, len(bridge_movie_crew) + 1)
        bridge_movie_crew["lineage_id"] = range(1, len(bridge_movie_crew) + 1)  # Add lineage_id
        bridge_movie_crew["created_at"] = current_time
        bridge_movie_crew["updated_at"] = current_time
        bridge_movie_crew = bridge_movie_crew[["bridge_id", "movie_id", "crew_id", "lineage_id", "character_name", "created_at", "updated_at"]]
        
        for _, row in bridge_movie_crew.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["bridge_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_bridge_movie_crew",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Fact: fact_movie_metrics
        fact_movie_metrics = silver_df[["movie_id", "date_x", "country", "orig_lang", "budget_x", "revenue", "score"]]
        fact_movie_metrics = fact_movie_metrics.merge(dim_date[["release_date", "date_id"]], left_on="date_x", right_on="release_date", how="left")
        fact_movie_metrics = fact_movie_metrics.merge(dim_country[["country_name", "country_id"]], left_on="country", right_on="country_name", how="left")
        fact_movie_metrics = fact_movie_metrics.merge(dim_language[["language_name", "language_id"]], left_on="orig_lang", right_on="language_name", how="left")
        fact_movie_metrics = fact_movie_metrics.drop_duplicates(subset=["movie_id", "date_id", "country_id", "language_id"])
        fact_movie_metrics["fact_id"] = range(1, len(fact_movie_metrics) + 1)
        fact_movie_metrics["lineage_id"] = range(1, len(fact_movie_metrics) + 1)  # Add lineage_id
        fact_movie_metrics["created_at"] = current_time
        fact_movie_metrics["updated_at"] = current_time
        fact_movie_metrics = fact_movie_metrics.rename(columns={"budget_x": "budget"})
        fact_movie_metrics = fact_movie_metrics[["fact_id", "movie_id", "date_id", "country_id", "language_id", "budget", "revenue", "score", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in fact_movie_metrics.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["fact_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_fact_movie_metrics",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Aggregate: revenue_by_genre
        movie_revenue = fact_movie_metrics[["movie_id", "revenue"]].drop_duplicates()
        revenue_by_genre = movie_revenue.merge(bridge_movie_genre, on="movie_id") \
                                        .merge(dim_genre, on="genre_id") \
                                        .groupby("genre_name")["revenue"].sum().reset_index() \
                                        .rename(columns={"revenue": "total_revenue"})
        revenue_by_genre["lineage_id"] = range(1, len(revenue_by_genre) + 1)  # Add lineage_id
        revenue_by_genre["created_at"] = current_time
        revenue_by_genre["updated_at"] = current_time

        # Log the aggregation transformation for each genre
        for _, row in revenue_by_genre.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "aggregated_revenue_by_genre",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
        
        # Aggregate: avg_score_by_year
        avg_score_by_year = fact_movie_metrics.merge(dim_date, on="date_id") \
                                            .groupby("year")["score"].mean().reset_index() \
                                            .rename(columns={"score": "avg_score"})
        avg_score_by_year["lineage_id"] = range(1, len(avg_score_by_year) + 1)  # Add lineage_id
        avg_score_by_year["created_at"] = current_time
        avg_score_by_year["updated_at"] = current_time
        
        for _, row in avg_score_by_year.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": "aggregate_avg_score_by_year",
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "aggregated_avg_score_by_year",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1
            
        # Lineage log table
        lineage_df = pd.DataFrame(lineage_entries)
        lineage_df = lineage_df.rename(columns={"record_id": "lineage_id"})
        
        return {
            "fact_movie_metrics": fact_movie_metrics,
            "dim_movie": dim_movie,
            "dim_date": dim_date,
            "dim_country": dim_country,
            "dim_language": dim_language,
            "dim_crew": dim_crew,
            "dim_genre": dim_genre,
            "bridge_movie_genre": bridge_movie_genre,
            "bridge_movie_crew": bridge_movie_crew,
            "revenue_by_genre": revenue_by_genre,
            "avg_score_by_year": avg_score_by_year,
            "lineage_log": lineage_df
        }