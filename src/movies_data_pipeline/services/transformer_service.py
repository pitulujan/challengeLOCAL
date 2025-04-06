import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import logging
import uuid
from pathlib import Path
import os

logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, bronze_file_path: str):
        """Initialize the Transformer with bronze file path."""
        self.bronze_file_path = Path(bronze_file_path)
        self.silver_file_path = Path(os.getenv("SILVER_BASE_PATH")) / "movies.parquet"
        if not os.getenv("SILVER_BASE_PATH"):
            raise ValueError("SILVER_BASE_PATH environment variable not set")

    def transform(self) -> Dict[str, pd.DataFrame]:
        logger.info("Starting transformation")
        try:
            if not self.bronze_file_path.exists():
                logger.info(f"Bronze file {self.bronze_file_path} does not exist; returning empty result")
                return {}
            bronze_df = pd.read_parquet(self.bronze_file_path)
            logger.debug(f"Loaded bronze data with shape {bronze_df.shape}")
        except Exception as e:
            logger.error(f"Failed to load bronze data: {str(e)}")
            raise

        if bronze_df.empty:
            logger.info("No new data to transform; returning empty result")
            return {}

        # Create silver layer
        silver_df, lineage_entries = self._create_silver_layer(bronze_df)
        
        # Save silver layer (optional, depending on your pipeline)
        silver_df.to_parquet(self.silver_file_path, index=False)
        
        # Create gold tables with proper column selection
        gold_tables = self._create_gold_tables(silver_df, lineage_entries)
        
        logger.info("Transformation completed successfully")
        return gold_tables

    def _create_silver_layer(self, df: pd.DataFrame) -> tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Create silver layer by cleaning and standardizing bronze data with lineage tracking."""
        df = self._standardize_columns(df)
        lineage_entries = [{
            "lineage_id": row["uuid"],
            "source_path": str(self.bronze_file_path),
            "stage": "bronze",
            "transformation": "loaded",
            "timestamp": datetime.now()
        } for _, row in df.iterrows()]
        
        df = self._process_dates(df)
        for entry in lineage_entries:
            entry["stage"] = "silver"
            entry["transformation"] = "dates_processed"
            entry["timestamp"] = datetime.now()
        
        df = self._process_genre_and_crew(df)
        for entry in lineage_entries:
            entry["transformation"] = "genre_crew_processed"
            entry["timestamp"] = datetime.now()
        
        current_time = datetime.now()
        df["created_at"] = current_time
        df["updated_at"] = current_time
        df["lineage_id"] = df["uuid"]
        
        lineage_entries = [{
            "lineage_id": row["lineage_id"],
            "source_path": str(self.bronze_file_path),
            "stage": "silver",
            "transformation": "finalized",
            "timestamp": current_time
        } for _, row in df.iterrows()]
        
        logger.debug(f"Silver DF columns: {list(df.columns)}")
        return df, lineage_entries

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names in the DataFrame and ensure 'name' and 'orig_title' are not null."""
        logger.debug(f"Initial DataFrame columns: {list(df.columns)}")
        logger.debug(f"Initial DataFrame head:\n{df.head().to_string()}")
        
        # Handle 'names' column if present
        if "names" in df.columns:
            if "name" not in df.columns:
                df["name"] = df["names"]
            else:
                df["name"] = df["name"].fillna(df["names"])
            df = df.drop(columns=["names"])
            logger.debug("Handled 'names' column")
        
        # Ensure 'name' column exists
        if "name" not in df.columns:
            raise KeyError("Input data must contain a 'name' column for movie titles.")
        
        # Ensure 'orig_title' column exists, default to 'name' if missing
        if "orig_title" not in df.columns:
            logger.warning("Input data does not contain 'orig_title'; filling with 'name'")
            df["orig_title"] = df["name"]
        
        logger.debug(f"After initial standardization:\n{df[['name', 'orig_title']].head().to_string()}")
        
        # Fill null values in 'name' with 'orig_title'
        if df["name"].isnull().any():
            logger.warning(f"Found null values in 'name' column:\n{df[df['name'].isnull()][['name', 'orig_title']].to_string()}")
            df["name"] = df["name"].fillna(df["orig_title"])
        
        # Fill null values in 'orig_title' with 'name'
        if df["orig_title"].isnull().any():
            logger.warning(f"Found null values in 'orig_title' column:\n{df[df['orig_title'].isnull()][['name', 'orig_title']].to_string()}")
            df["orig_title"] = df["orig_title"].fillna(df["name"])
        
        # If both are still null, use a default value
        df["name"] = df["name"].fillna("Unknown")
        df["orig_title"] = df["orig_title"].fillna("Unknown")
        
        logger.debug(f"DataFrame after filling nulls:\n{df[['name', 'orig_title']].to_string()}")
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
        """Create gold layer tables respecting unique constraints."""
        logger.debug(f"Silver DF columns before gold transformation: {list(silver_df.columns)}")
        current_time = datetime.now()
        
        # Dimension: dim_movie (unique on name, orig_title)
        dim_movie = silver_df[["uuid", "name", "orig_title", "overview", "status", "lineage_id"]].drop_duplicates(subset=["name", "orig_title"])
        if dim_movie["name"].isnull().any() or dim_movie["orig_title"].isnull().any():
            logger.error(f"Rows with null values in dim_movie:\n{dim_movie[dim_movie['name'].isnull() | dim_movie['orig_title'].isnull()][['uuid', 'name', 'orig_title', 'lineage_id']]}")
            raise ValueError("dim_movie contains null values in 'name' or 'orig_title' after standardization")
        dim_movie["movie_id"] = range(1, len(dim_movie) + 1)
        dim_movie["created_at"] = current_time
        dim_movie["updated_at"] = current_time
        dim_movie = dim_movie[["movie_id", "name", "orig_title", "overview", "status", "uuid", "lineage_id", "created_at", "updated_at"]]
        
        # Assign movie_id to silver_df based on name and orig_title
        silver_df = silver_df.merge(dim_movie[["name", "orig_title", "movie_id"]], on=["name", "orig_title"], how="left")
        if silver_df["movie_id"].isnull().any():
            logger.error(f"Rows with null movie_id in silver_df after merge:\n{silver_df[silver_df['movie_id'].isnull()][['uuid', 'name', 'orig_title', 'lineage_id']]}")
            raise ValueError("Failed to assign movie_id to all rows in silver_df")
        
        # Dimension: dim_date (unique on year, month, day)
        dim_date = silver_df[["date_x"]].drop_duplicates()
        dim_date["release_date"] = pd.to_datetime(dim_date["date_x"])
        dim_date["year"] = dim_date["release_date"].dt.year
        dim_date["month"] = dim_date["release_date"].dt.month
        dim_date["day"] = dim_date["release_date"].dt.day
        dim_date = dim_date.drop_duplicates(subset=["year", "month", "day"])
        dim_date["date_id"] = range(1, len(dim_date) + 1)
        dim_date = dim_date.drop(columns=["date_x"])
        dim_date["lineage_id"] = [str(uuid.uuid4()) for _ in range(len(dim_date))]
        dim_date["created_at"] = current_time
        dim_date["updated_at"] = current_time
        dim_date = dim_date[["date_id", "release_date", "year", "month", "day", "lineage_id", "created_at", "updated_at"]]
        
        # Dimension: dim_country (unique on country_name)
        dim_country = silver_df[["country"]].drop_duplicates()
        dim_country = dim_country.rename(columns={"country": "country_name"})
        dim_country["country_id"] = range(1, len(dim_country) + 1)
        dim_country["lineage_id"] = [str(uuid.uuid4()) for _ in range(len(dim_country))]
        dim_country["created_at"] = current_time
        dim_country["updated_at"] = current_time
        dim_country = dim_country[["country_id", "country_name", "lineage_id", "created_at", "updated_at"]]
        
        # Dimension: dim_language (unique on language_name)
        dim_language = silver_df[["orig_lang"]].drop_duplicates()
        dim_language = dim_language.rename(columns={"orig_lang": "language_name"})
        dim_language["language_id"] = range(1, len(dim_language) + 1)
        dim_language["lineage_id"] = [str(uuid.uuid4()) for _ in range(len(dim_language))]
        dim_language["created_at"] = current_time
        dim_language["updated_at"] = current_time
        dim_language = dim_language[["language_id", "language_name", "lineage_id", "created_at", "updated_at"]]
        
        # Dimension: dim_crew (unique on actor_name, character_name)
        crew_flat = silver_df.explode("crew_pairs")[["crew_pairs"]].dropna()
        dim_crew = pd.DataFrame(crew_flat["crew_pairs"].tolist())
        dim_crew = dim_crew.drop_duplicates(subset=["actor_name", "character_name"])
        dim_crew["crew_id"] = range(1, len(dim_crew) + 1)
        dim_crew["lineage_id"] = [str(uuid.uuid4()) for _ in range(len(dim_crew))]
        dim_crew["created_at"] = current_time
        dim_crew["updated_at"] = current_time
        dim_crew = dim_crew[["crew_id", "actor_name", "character_name", "lineage_id", "created_at", "updated_at"]]
        
        # Dimension: dim_genre (unique on genre_name)
        dim_genre = silver_df.explode("genre_list")[["genre_list"]].drop_duplicates()
        dim_genre = dim_genre.rename(columns={"genre_list": "genre_name"})
        dim_genre["genre_id"] = range(1, len(dim_genre) + 1)
        dim_genre["lineage_id"] = [str(uuid.uuid4()) for _ in range(len(dim_genre))]
        dim_genre["created_at"] = current_time
        dim_genre["updated_at"] = current_time
        dim_genre = dim_genre[["genre_id", "genre_name", "lineage_id", "created_at", "updated_at"]]
        
        # Bridge: bridge_movie_genre (unique on movie_id, genre_id)
        bridge_movie_genre = silver_df.explode("genre_list")[["movie_id", "genre_list", "lineage_id"]]
        bridge_movie_genre = bridge_movie_genre.merge(dim_genre[["genre_id", "genre_name"]], left_on="genre_list", right_on="genre_name", how="left")
        bridge_movie_genre = bridge_movie_genre.drop_duplicates(subset=["movie_id", "genre_id"])
        bridge_movie_genre["bridge_id"] = range(1, len(bridge_movie_genre) + 1)
        bridge_movie_genre = bridge_movie_genre[["bridge_id", "movie_id", "genre_id", "lineage_id"]]
        bridge_movie_genre["created_at"] = current_time
        bridge_movie_genre["updated_at"] = current_time
        bridge_movie_genre = bridge_movie_genre[["bridge_id", "movie_id", "genre_id", "lineage_id", "created_at", "updated_at"]]
        logger.debug(f"Bridge movie genre columns: {list(bridge_movie_genre.columns)}")
        
        # Bridge: bridge_movie_crew (unique on movie_id, crew_id, character_name)
        bridge_movie_crew = silver_df.explode("crew_pairs")[["movie_id", "crew_pairs", "lineage_id"]]
        bridge_movie_crew = bridge_movie_crew.dropna(subset=["crew_pairs"])
        bridge_movie_crew = bridge_movie_crew.reset_index(drop=True)
        crew_expanded = pd.DataFrame(bridge_movie_crew["crew_pairs"].tolist())
        bridge_movie_crew = pd.concat([bridge_movie_crew[["movie_id", "lineage_id"]], crew_expanded], axis=1)
        bridge_movie_crew = bridge_movie_crew.merge(dim_crew[["crew_id", "actor_name", "character_name"]], on=["actor_name", "character_name"], how="left")
        bridge_movie_crew = bridge_movie_crew.drop_duplicates(subset=["movie_id", "crew_id", "character_name"])
        bridge_movie_crew["bridge_id"] = range(1, len(bridge_movie_crew) + 1)
        bridge_movie_crew = bridge_movie_crew[["bridge_id", "movie_id", "crew_id", "character_name", "lineage_id"]]
        bridge_movie_crew["created_at"] = current_time
        bridge_movie_crew["updated_at"] = current_time
        bridge_movie_crew = bridge_movie_crew[["bridge_id", "movie_id", "crew_id", "character_name", "lineage_id", "created_at", "updated_at"]]
        
        # Fact: fact_movie_metrics (unique on movie_id, date_id, country_id, language_id)
        fact_movie_metrics = silver_df[["movie_id", "date_x", "country", "orig_lang", "budget_x", "revenue", "score", "lineage_id"]]
        fact_movie_metrics = fact_movie_metrics.merge(dim_date[["release_date", "date_id"]], left_on="date_x", right_on="release_date", how="left")
        fact_movie_metrics = fact_movie_metrics.merge(dim_country[["country_name", "country_id"]], left_on="country", right_on="country_name", how="left")
        fact_movie_metrics = fact_movie_metrics.merge(dim_language[["language_name", "language_id"]], left_on="orig_lang", right_on="language_name", how="left")
        fact_movie_metrics = fact_movie_metrics.drop_duplicates(subset=["movie_id", "date_id", "country_id", "language_id"])
        fact_movie_metrics["fact_id"] = range(1, len(fact_movie_metrics) + 1)
        fact_movie_metrics = fact_movie_metrics[["fact_id", "movie_id", "date_id", "country_id", "language_id", "budget_x", "revenue", "score", "lineage_id"]]
        fact_movie_metrics = fact_movie_metrics.rename(columns={"budget_x": "budget"})
        fact_movie_metrics["created_at"] = current_time
        fact_movie_metrics["updated_at"] = current_time
        fact_movie_metrics = fact_movie_metrics[["fact_id", "movie_id", "date_id", "country_id", "language_id", "budget", "revenue", "score", "lineage_id", "created_at", "updated_at"]]
        
        # Lineage log table
        lineage_df = pd.DataFrame(lineage_entries)
        lineage_df["lineage_log_id"] = range(1, len(lineage_df) + 1)
        lineage_df = lineage_df[["lineage_log_id", "lineage_id", "source_path", "stage", "transformation", "timestamp"]]
        
        # Add lineage entries for gold transformation
        for table_name, table_df in {
            "dim_movie": dim_movie, "dim_date": dim_date, "dim_country": dim_country,
            "dim_language": dim_language, "dim_crew": dim_crew, "dim_genre": dim_genre,
            "bridge_movie_genre": bridge_movie_genre, "bridge_movie_crew": bridge_movie_crew,
            "fact_movie_metrics": fact_movie_metrics
        }.items():
            for _, row in table_df.iterrows():
                lineage_df = pd.concat([lineage_df, pd.DataFrame([{
                    "lineage_log_id": len(lineage_df) + 1,
                    "lineage_id": row["lineage_id"],
                    "source_path": str(self.bronze_file_path),
                    "stage": "gold",
                    "transformation": f"created_{table_name}",
                    "timestamp": current_time
                }])], ignore_index=True)
        
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
            "lineage_log": lineage_df
        }