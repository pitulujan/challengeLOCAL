import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import logging
from pathlib import Path
import os
from .extractor_service import Extractor

logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, bronze_file_path: str, silver_file_path: str = None):
        """Initialize the Transformer with bronze and silver file paths."""
        self.bronze_file_path = Path(bronze_file_path)
        self.silver_file_path = Path(os.getenv("SILVER_BASE_PATH")) / "silver_movies.parquet"

    def transform(self, new_file_path: str) -> Dict[str, pd.DataFrame]:
        """Transform only the newly uploaded file's data to silver and gold layers."""
        logger.info("Starting transformation for new file")
        
        # Extract only the new file's data
        try:
            new_df, record_count = Extractor(self.bronze_file_path).extract(new_file_path)
            if record_count == 0:
                logger.info(f"No new data in {new_file_path}; skipping transformation")
                return {}
        except Exception as e:
            logger.error(f"Failed to extract new data from {new_file_path}: {str(e)}")
            raise

        # Create silver layer with deduplication
        silver_df, new_silver_df, lineage_entries = self._create_silver_layer(new_df, new_file_path)
        
        # Save updated silver layer (even if no new records, to update timestamps)
        silver_df.to_parquet(self.silver_file_path, index=False)
        logger.info(f"Updated silver layer with {len(new_silver_df)} new unique records")
        
        # If no new records, skip gold table creation
        if new_silver_df.empty:
            logger.info("No new unique records to process into gold layer; skipping")
            return {}
        
        # Create gold tables from new deduplicated records
        gold_tables = self._create_gold_tables(new_silver_df, lineage_entries)
        
        logger.info("Transformation completed successfully")
        return gold_tables

    def _create_silver_layer(self, new_df: pd.DataFrame, new_file_path: str) -> tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
        """Create silver layer by processing new data, deduplicating against existing silver data."""
        logger.info("Creating silver layer from new data")
        
        # Standardize new data
        new_df = self._standardize_columns(new_df)
        new_df["bronze_id"] = range(1, len(new_df) + 1)  # Temporary bronze_id for new data
        
        # Process dates to ensure 'release_date' exists before deduplication
        new_df = self._process_dates(new_df)
        
        # Load existing silver data
        if self.silver_file_path.exists():
            existing_silver = pd.read_parquet(self.silver_file_path)
            if "release_date" not in existing_silver.columns:
                existing_silver = self._process_dates(existing_silver)
            if "crew_pairs" not in existing_silver.columns:
                existing_silver = self._process_genre_and_crew(existing_silver)
            logger.info(f"Loaded {len(existing_silver)} existing silver records")
        else:
            existing_silver = pd.DataFrame(columns=new_df.columns)
            logger.info("No existing silver data; starting fresh")
        
        # Combine new and existing data
        combined_df = pd.concat([existing_silver, new_df], ignore_index=True)
        
        # Process genre and crew on the combined data before deduplication
        combined_df = self._process_genre_and_crew(combined_df)
        
        # Define unique key for deduplication
        unique_key = ["name", "orig_title"]
        
        # Check if all unique key columns exist
        missing_cols = [col for col in unique_key if col not in combined_df.columns]
        if missing_cols:
            logger.error(f"Missing columns in combined_df for deduplication: {missing_cols}")
            raise KeyError(f"Missing columns for unique key: {missing_cols}")
        
        # Identify duplicates
        duplicates = combined_df[combined_df.duplicated(subset=unique_key, keep="first")]
        if not duplicates.empty:
            logger.info(f"Found {len(duplicates)} duplicate records based on {unique_key}")
            # for _, dup in duplicates.iterrows():
            #     logger.debug(f"Duplicate record: {dup[unique_key].to_dict()}")

        # Keep only unique records (first occurrence)
        silver_df = combined_df.drop_duplicates(subset=unique_key, keep="first")
        
        # Identify new unique records added from this upload
        new_silver_df = silver_df[~silver_df["bronze_id"].isin(existing_silver["bronze_id"])]
        logger.info(f"Identified {len(new_silver_df)} new unique records")
        
        # Add timestamps and silver_id to full silver_df using .loc to avoid warnings
        current_time = datetime.now()
        silver_df.loc[:, "created_at"] = silver_df["created_at"].fillna(current_time)
        silver_df.loc[:, "updated_at"] = current_time
        silver_df.loc[:, "silver_id"] = range(1, len(silver_df) + 1)
        
        # Lineage tracking for new records
        lineage_entries = []
        for _, row in new_silver_df.iterrows():
            lineage_entries.append({
                "lineage_log_id": len(lineage_entries) + 1,
                "record_id": row["bronze_id"],
                "source_path": new_file_path,
                "stage": "silver",
                "transformation": "deduplicated_and_processed",
                "timestamp": current_time
            })
        
        return silver_df, new_silver_df, lineage_entries

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
            df = df.rename(columns={date_col: "release_date"})
            df["release_date"] = df["release_date"].astype(str).str.strip()
            df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        else:
            logger.error("Input data must contain a date column ('date_x', 'release_date', or 'date')")
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
        """Create gold layer tables from silver records without generating IDs in Python."""
        current_time = datetime.now()
        lineage_log_id_counter = max(entry["lineage_log_id"] for entry in lineage_entries) + 1 if lineage_entries else 1

        ### Dimension Tables ###
        # DimMovie: Use name and orig_title as natural keys for lineage_id
        dim_movie = silver_df[["name", "orig_title", "overview", "status"]].drop_duplicates(subset=["name", "orig_title"])
        dim_movie["lineage_id"] = dim_movie["name"] + "_" + dim_movie["orig_title"]
        dim_movie["created_at"] = current_time
        dim_movie["updated_at"] = current_time
        dim_movie = dim_movie[["name", "orig_title", "overview", "status", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_movie.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_movie",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # DimDate: Use year, month, day as natural keys
        dim_date = silver_df[["release_date"]].drop_duplicates()
        dim_date["year"] = dim_date["release_date"].dt.year
        dim_date["month"] = dim_date["release_date"].dt.month
        dim_date["day"] = dim_date["release_date"].dt.day
        dim_date["lineage_id"] = dim_date["year"].astype(str) + "_" + dim_date["month"].astype(str) + "_" + dim_date["day"].astype(str)
        dim_date["created_at"] = current_time
        dim_date["updated_at"] = current_time
        dim_date = dim_date[["release_date", "year", "month", "day", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_date.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_date",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # DimCountry: Use country_name as natural key
        dim_country = silver_df[["country"]].drop_duplicates()
        dim_country = dim_country.rename(columns={"country": "country_name"})
        dim_country["lineage_id"] = dim_country["country_name"]
        dim_country["created_at"] = current_time
        dim_country["updated_at"] = current_time
        dim_country = dim_country[["country_name", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_country.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_country",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # DimLanguage: Use language_name as natural key
        dim_language = silver_df[["orig_lang"]].drop_duplicates()
        dim_language = dim_language.rename(columns={"orig_lang": "language_name"})
        dim_language["lineage_id"] = dim_language["language_name"]
        dim_language["created_at"] = current_time
        dim_language["updated_at"] = current_time
        dim_language = dim_language[["language_name", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_language.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_language",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # DimCrew: Use actor_name and character_name as natural keys
        crew_flat = silver_df.explode("crew_pairs").dropna(subset=["crew_pairs"])
        dim_crew = pd.DataFrame(crew_flat["crew_pairs"].tolist())
        dim_crew = dim_crew.drop_duplicates(subset=["actor_name", "character_name"])
        dim_crew["lineage_id"] = dim_crew["actor_name"] + "_" + dim_crew["character_name"]
        dim_crew["created_at"] = current_time
        dim_crew["updated_at"] = current_time
        dim_crew = dim_crew[["actor_name", "character_name", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_crew.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_crew",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # DimGenre: Use genre_name as natural key
        dim_genre = silver_df.explode("genre_list")[["genre_list"]].drop_duplicates()
        dim_genre = dim_genre.rename(columns={"genre_list": "genre_name"})
        dim_genre["lineage_id"] = dim_genre["genre_name"]
        dim_genre["created_at"] = current_time
        dim_genre["updated_at"] = current_time
        dim_genre = dim_genre[["genre_name", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in dim_genre.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_dim_genre",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        ### Bridge and Fact Tables ###
        # BridgeMovieGenre: Deduplicate by movie_lineage_id and genre_lineage_id
        bridge_movie_genre = silver_df.explode("genre_list")[["name", "orig_title", "genre_list"]].drop_duplicates()
        bridge_movie_genre = bridge_movie_genre.merge(
            dim_movie[["name", "orig_title", "lineage_id"]], 
            on=["name", "orig_title"], 
            how="left"
        ).rename(columns={"lineage_id": "movie_lineage_id"})
        bridge_movie_genre = bridge_movie_genre.merge(
            dim_genre[["genre_name", "lineage_id"]], 
            left_on="genre_list", 
            right_on="genre_name", 
            how="left"
        ).rename(columns={"lineage_id": "genre_lineage_id"})
        bridge_movie_genre["lineage_id"] = bridge_movie_genre["movie_lineage_id"] + "_" + bridge_movie_genre["genre_lineage_id"]
        bridge_movie_genre["created_at"] = current_time
        bridge_movie_genre["updated_at"] = current_time
        bridge_movie_genre = bridge_movie_genre[["movie_lineage_id", "genre_lineage_id", "lineage_id", "created_at", "updated_at"]]
        # Deduplicate
        bridge_movie_genre = bridge_movie_genre.drop_duplicates(subset=["movie_lineage_id", "genre_lineage_id"], keep="last")
        logger.info(f"Deduplicated bridge_movie_genre to {len(bridge_movie_genre)} unique records")
        
        for _, row in bridge_movie_genre.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_bridge_movie_genre",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # BridgeMovieCrew: Deduplicate by movie_lineage_id, crew_lineage_id, and character_name
        bridge_movie_crew = silver_df.explode("crew_pairs")[["name", "orig_title", "crew_pairs"]].dropna(subset=["crew_pairs"])
        bridge_movie_crew = bridge_movie_crew.reset_index(drop=True)
        crew_expanded = pd.DataFrame(bridge_movie_crew["crew_pairs"].tolist())
        bridge_movie_crew = pd.concat([bridge_movie_crew[["name", "orig_title"]], crew_expanded], axis=1)
        bridge_movie_crew = bridge_movie_crew.merge(
            dim_movie[["name", "orig_title", "lineage_id"]], 
            on=["name", "orig_title"], 
            how="left"
        ).rename(columns={"lineage_id": "movie_lineage_id"})
        bridge_movie_crew = bridge_movie_crew.merge(
            dim_crew[["actor_name", "character_name", "lineage_id"]], 
            on=["actor_name", "character_name"], 
            how="left"
        ).rename(columns={"lineage_id": "crew_lineage_id"})
        bridge_movie_crew["lineage_id"] = bridge_movie_crew["movie_lineage_id"] + "_" + bridge_movie_crew["crew_lineage_id"]
        bridge_movie_crew["created_at"] = current_time
        bridge_movie_crew["updated_at"] = current_time
        bridge_movie_crew = bridge_movie_crew[["movie_lineage_id", "crew_lineage_id", "character_name", "lineage_id", "created_at", "updated_at"]]
        # Deduplicate
        bridge_movie_crew = bridge_movie_crew.drop_duplicates(subset=["movie_lineage_id", "crew_lineage_id", "character_name"], keep="last")
        logger.info(f"Deduplicated bridge_movie_crew to {len(bridge_movie_crew)} unique records")
        
        for _, row in bridge_movie_crew.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_bridge_movie_crew",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # FactMovieMetrics: Deduplicate by movie_lineage_id, date_lineage_id, country_lineage_id, language_lineage_id
        fact_movie_metrics = silver_df[["name", "orig_title", "release_date", "country", "orig_lang", "budget_x", "revenue", "score"]]
        fact_movie_metrics = fact_movie_metrics.merge(
            dim_movie[["name", "orig_title", "lineage_id"]], 
            on=["name", "orig_title"], 
            how="left"
        ).rename(columns={"lineage_id": "movie_lineage_id"})
        fact_movie_metrics = fact_movie_metrics.merge(
            dim_date[["release_date", "lineage_id"]], 
            on="release_date", 
            how="left"
        ).rename(columns={"lineage_id": "date_lineage_id"})
        fact_movie_metrics = fact_movie_metrics.merge(
            dim_country[["country_name", "lineage_id"]], 
            left_on="country", 
            right_on="country_name", 
            how="left"
        ).rename(columns={"lineage_id": "country_lineage_id"})
        fact_movie_metrics = fact_movie_metrics.merge(
            dim_language[["language_name", "lineage_id"]], 
            left_on="orig_lang", 
            right_on="language_name", 
            how="left"
        ).rename(columns={"lineage_id": "language_lineage_id"})
        fact_movie_metrics["lineage_id"] = (
            fact_movie_metrics["movie_lineage_id"] + "_" + 
            fact_movie_metrics["date_lineage_id"] + "_" + 
            fact_movie_metrics["country_lineage_id"] + "_" + 
            fact_movie_metrics["language_lineage_id"]
        )
        fact_movie_metrics["created_at"] = current_time
        fact_movie_metrics["updated_at"] = current_time
        fact_movie_metrics = fact_movie_metrics.rename(columns={"budget_x": "budget"})
        fact_movie_metrics = fact_movie_metrics[[
            "movie_lineage_id", "date_lineage_id", "country_lineage_id", "language_lineage_id",
            "budget", "revenue", "score", "lineage_id", "created_at", "updated_at"
        ]]
        # Deduplicate
        fact_movie_metrics = fact_movie_metrics.drop_duplicates(
            subset=["movie_lineage_id", "date_lineage_id", "country_lineage_id", "language_lineage_id"], 
            keep="last"
        )
        logger.info(f"Deduplicated fact_movie_metrics to {len(fact_movie_metrics)} unique records")
        
        for _, row in fact_movie_metrics.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "created_fact_movie_metrics",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        ### Aggregate Tables ###
        # RevenueByGenre: Use genre_name as natural key
        temp_movie_revenue = fact_movie_metrics[["movie_lineage_id", "revenue"]].drop_duplicates()
        temp_bridge = bridge_movie_genre[["movie_lineage_id", "genre_lineage_id"]].drop_duplicates()
        revenue_by_genre = temp_movie_revenue.merge(temp_bridge, on="movie_lineage_id").merge(
            dim_genre[["genre_name", "lineage_id"]], 
            left_on="genre_lineage_id", 
            right_on="lineage_id"
        ).groupby("genre_name")["revenue"].sum().reset_index().rename(columns={"revenue": "total_revenue"})
        revenue_by_genre["lineage_id"] = revenue_by_genre["genre_name"]
        revenue_by_genre["created_at"] = current_time
        revenue_by_genre["updated_at"] = current_time
        revenue_by_genre = revenue_by_genre[["genre_name", "total_revenue", "lineage_id", "created_at", "updated_at"]]
        
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

        # AvgScoreByYear: Use year as natural key
        avg_score_by_year = fact_movie_metrics.merge(
            dim_date[["lineage_id", "year"]], 
            left_on="date_lineage_id", 
            right_on="lineage_id"
        ).groupby("year")["score"].mean().reset_index().rename(columns={"score": "avg_score"})
        avg_score_by_year["lineage_id"] = avg_score_by_year["year"].astype(str)
        avg_score_by_year["created_at"] = current_time
        avg_score_by_year["updated_at"] = current_time
        avg_score_by_year = avg_score_by_year[["year", "avg_score", "lineage_id", "created_at", "updated_at"]]
        
        for _, row in avg_score_by_year.iterrows():
            lineage_entries.append({
                "lineage_log_id": lineage_log_id_counter,
                "record_id": row["lineage_id"],
                "source_path": str(self.bronze_file_path),
                "stage": "gold",
                "transformation": "aggregated_avg_score_by_year",
                "timestamp": current_time
            })
            lineage_log_id_counter += 1

        # LineageLog: Use lineage_id from other tables
        lineage_df = pd.DataFrame(lineage_entries)
        lineage_df = lineage_df.rename(columns={"record_id": "lineage_id"})
        
        return {
            "dim_movie": dim_movie,
            "dim_date": dim_date,
            "dim_country": dim_country,
            "dim_language": dim_language,
            "dim_crew": dim_crew,
            "dim_genre": dim_genre,
            "bridge_movie_genre": bridge_movie_genre,
            "bridge_movie_crew": bridge_movie_crew,
            "fact_movie_metrics": fact_movie_metrics,
            "revenue_by_genre": revenue_by_genre,
            "avg_score_by_year": avg_score_by_year,
            "lineage_log": lineage_df
        }