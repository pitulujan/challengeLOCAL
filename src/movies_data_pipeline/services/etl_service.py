import pandas as pd
import os
from typing import Dict
from fastapi import UploadFile
from datetime import datetime
from movies_data_pipeline.services.search_service import SearchService
from movies_data_pipeline.data_access.database import get_session_direct
import logging
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ETLService:
    def __init__(self):
        self.bronze_path = "src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet"
        self.silver_base_path = "src/movies_data_pipeline/data_access/data_lake/silver/"
        self.gold_base_path = "src/movies_data_pipeline/data_access/data_lake/gold/"
        self.search_service = SearchService()

    def extract(self, file: UploadFile) -> pd.DataFrame:
        """
        Extract data from an uploaded file and store it as Parquet in the bronze layer.

        Args:
            file (UploadFile): The uploaded file (CSV, JSON, or PDF).

        Returns:
            pd.DataFrame: The extracted DataFrame for further processing.

        Raises:
            ValueError: If the file type is unsupported or PDF extraction fails.
        """
        file_type = file.filename.split(".")[-1].lower()

        if file_type == "csv":
            df = pd.read_csv(file.file)
        elif file_type == "json":
            df = pd.read_json(file.file)
        else:
            raise ValueError("Unsupported file type. Use 'csv', 'json', or 'pdf'.") 

        # Add timestamps for tracking
        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time

        # Append to existing Parquet file if it exists
        if os.path.exists(self.bronze_path):
            existing_df = pd.read_parquet(self.bronze_path)
            # Ensure timestamp columns exist in existing data
            if 'created_at' not in existing_df.columns:
                existing_df['created_at'] = pd.NaT
            if 'updated_at' not in existing_df.columns:
                existing_df['updated_at'] = pd.NaT
            df = pd.concat([existing_df, df], ignore_index=True)

        # Save to bronze layer as Parquet
        df.to_parquet(self.bronze_path, index=False)
        return df

    def transform(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        raw_df = pd.read_parquet(self.bronze_path)
        print("Sample raw date_x:", raw_df["date_x"].head(5).tolist())

        # Standardize column names
        if "names" in raw_df.columns and "name" not in raw_df.columns:
            raw_df = raw_df.rename(columns={"names": "name"})
        elif "name" not in raw_df.columns:
            raise KeyError("Input data must contain a 'name' or 'names' column for movie titles.")

        # Robust date parsing
        possible_date_cols = ["date_x", "release_date", "date"]
        date_col = next((col for col in possible_date_cols if col in raw_df.columns), None)
        if date_col:
            raw_df = raw_df.rename(columns={date_col: "date_x"})
            raw_df["date_x"] = raw_df["date_x"].str.strip()
            raw_df["date_x"] = pd.to_datetime(raw_df["date_x"], format="%m/%d/%Y", errors="coerce")
            print("Parsed date_x:", raw_df["date_x"].head(5).tolist())
        else:
            raise KeyError("Input data must contain a date column ('date_x', 'release_date', or 'date')")

        raw_df["genre_list"] = raw_df["genre"].str.split(",\s+")

        # --- Parse crew data correctly ---
        def parse_crew(crew_str):
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

        raw_df["crew_pairs"] = raw_df["crew"].apply(parse_crew)

        # --- Silver Layer: Dimension Tables ---
        dim_date = raw_df[["date_x"]].drop_duplicates().reset_index(drop=True)
        dim_date["date_id"] = dim_date.index + 1
        dim_date["year"] = dim_date["date_x"].dt.year
        dim_date["month"] = dim_date["date_x"].dt.month
        dim_date["day"] = dim_date["date_x"].dt.day
        dim_date["quarter"] = dim_date["date_x"].dt.quarter

        genres = raw_df["genre_list"].explode().dropna().unique()
        dim_genre = pd.DataFrame({"genre_name": genres})
        dim_genre["genre_id"] = dim_genre.index + 1

        dim_language = raw_df[["orig_lang"]].drop_duplicates().reset_index(drop=True)
        dim_language.columns = ["language_name"]
        dim_language["language_id"] = dim_language.index + 1

        dim_country = raw_df[["country"]].drop_duplicates().reset_index(drop=True)
        dim_country.columns = ["country_name"]
        dim_country["country_id"] = dim_country.index + 1

        dim_movie = raw_df.merge(dim_date, on="date_x", suffixes=('_raw', '_date')) \
                        .merge(dim_language, left_on="orig_lang", right_on="language_name") \
                        .merge(dim_country, left_on="country", right_on="country_name")
        dim_movie["movie_id"] = dim_movie.index + 1
        dim_movie = dim_movie[["movie_id", "name", "orig_title", "overview", "status", "crew_pairs", "date_x", "date_id", "language_id", "country_id", "genre_list"]]

        # --- Silver Layer: Bridge Tables ---
        movie_genre_df = dim_movie[["movie_id", "genre_list"]].explode("genre_list").rename(columns={"genre_list": "genre_name"})
        movie_genre_df = movie_genre_df.dropna(subset=["genre_name"])
        movie_genre_df = movie_genre_df[movie_genre_df["genre_name"] != ""]
        bridge_movie_genre = movie_genre_df.merge(dim_genre, on="genre_name")
        bridge_movie_genre["movie_genre_id"] = bridge_movie_genre.index + 1
        bridge_movie_genre = bridge_movie_genre[["movie_genre_id", "movie_id", "genre_id"]]

        crew_df = dim_movie[["movie_id", "crew_pairs"]].explode("crew_pairs").reset_index(drop=True)
        crew_df = crew_df.dropna(subset=["crew_pairs"])
        crew_df = pd.concat([crew_df["movie_id"], crew_df["crew_pairs"].apply(pd.Series)], axis=1)
        crew_df["role"] = "Actor"

        dim_crew = crew_df[["actor_name"]].drop_duplicates().reset_index(drop=True)
        dim_crew.columns = ["crew_name"]
        dim_crew["crew_id"] = dim_crew.index + 1

        dim_role = pd.DataFrame({"role": ["Actor"]}).reset_index(drop=True)
        dim_role["role_id"] = dim_role.index + 1

        bridge_movie_crew = crew_df.merge(dim_crew, left_on="actor_name", right_on="crew_name") \
                                .merge(dim_role, on="role")
        bridge_movie_crew["movie_crew_id"] = bridge_movie_crew.index + 1
        bridge_movie_crew = bridge_movie_crew[["movie_crew_id", "movie_id", "crew_id", "role_id", "character_name"]]

        # --- Silver Layer: Fact Table ---
        fact_movie_performance = raw_df.merge(dim_movie, on=["name", "date_x", "orig_title"])
        fact_movie_performance["financial_id"] = fact_movie_performance.index + 1
        fact_movie_performance["profit"] = fact_movie_performance["revenue"] - fact_movie_performance["budget_x"]
        fact_movie_performance = fact_movie_performance[[
            "financial_id", "movie_id", "date_id", "language_id", "country_id",
            "score", "budget_x", "revenue", "profit"
        ]].rename(columns={"budget_x": "budget"})

        # --- Index Movies into Typesense ---
        movies_to_index = dim_movie.merge(bridge_movie_genre, on="movie_id", suffixes=('_movie', '_bridge')) \
                                .merge(dim_genre, on="genre_id", suffixes=('_movie', '_genre')) \
                                .merge(dim_date, on="date_id", suffixes=('_movie', '_date')) \
                                .merge(dim_language, on="language_id", suffixes=('_movie', '_lang')) \
                                .merge(dim_country, on="country_id", suffixes=('_movie', '_country')) \
                                .merge(fact_movie_performance, on="movie_id", suffixes=('_movie', '_fact'))

        genres_by_movie = movies_to_index.groupby("movie_id")["genre_name"].apply(list).reset_index().rename(columns={"genre_name": "genres"})
        crew_by_movie = bridge_movie_crew.merge(dim_crew, on="crew_id").merge(dim_role, on="role_id")
        crew_by_movie["crew_entry"] = crew_by_movie.apply(
            lambda row: {"name": row["crew_name"], "role_name": row["role"], "character_name": row["character_name"]}, axis=1
        )
        crew_by_movie = crew_by_movie.groupby("movie_id")["crew_entry"].apply(list).reset_index()

        movies_to_index = movies_to_index.drop_duplicates(subset=["movie_id"]).merge(genres_by_movie, on="movie_id").merge(crew_by_movie, on="movie_id")

        for _, row in movies_to_index.iterrows():
            release_date = row["date_x_movie"].strftime("%Y-%m-%d") if pd.notna(row["date_x_movie"]) else "Unknown"
            movie_dict = {
                "name": row["name"],
                "orig_title": row["orig_title"],
                "overview": row["overview"],
                "status": row["status"],
                "release_date": release_date,
                "genres": row["genres"],
                "crew": row["crew_entry"],
                "country": row["country_name"],
                "language": row["language_name"],
                "budget": float(row["budget"]),
                "revenue": float(row["revenue"]),
                "score": float(row["score"]),
                "is_deleted": False
            }
            self.search_service.index_movie(movie_dict)

        # --- Gold Layer: Aggregate Tables ---
        revenue_by_genre = fact_movie_performance.merge(bridge_movie_genre, on="movie_id") \
                                                .merge(dim_genre, on="genre_id") \
                                                .groupby("genre_name")["revenue"].sum().reset_index() \
                                                .rename(columns={"revenue": "total_revenue"})

        merged_df = fact_movie_performance.merge(dim_movie, on="movie_id", suffixes=('_fact', '_movie'))
        avg_score_by_year = merged_df.merge(dim_date, left_on="date_id_movie", right_on="date_id", suffixes=('_movie', '_date')) \
                                    .groupby("year")["score"].mean().reset_index() \
                                    .rename(columns={"score": "avg_score"})

        # Add last_updated timestamp to gold tables
        current_time = datetime.now()
        revenue_by_genre['last_updated'] = current_time
        avg_score_by_year['last_updated'] = current_time

        return {
            "silver": {
                "dim_date": dim_date,
                "dim_genre": dim_genre,
                "dim_language": dim_language,
                "dim_country": dim_country,
                "dim_movie": dim_movie,
                "dim_crew": dim_crew,
                "dim_role": dim_role,
                "bridge_movie_genre": bridge_movie_genre,
                "bridge_movie_crew": bridge_movie_crew,
                "factMoviePerformance": fact_movie_performance
            },
            "gold": {
                "revenue_by_genre": revenue_by_genre,
                "avg_score_by_year": avg_score_by_year
            }
        }

    def load(self, transformed_data: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        """
        Load transformed data into the Silver and Gold layers.
        - Silver layer: Saved as Parquet files.
        - Gold layer: Saved as Parquet files and loaded into PostgreSQL tables.

        Args:
            transformed_data (Dict[str, Dict[str, pd.DataFrame]]): Dictionary containing 'silver' and 'gold' data.
        """
        for table_name, df in transformed_data["silver"].items():
            df.to_parquet(f"{self.silver_base_path}{table_name}.parquet", index=False)

        with get_session_direct() as session:
            for table_name, df in transformed_data["gold"].items():
                df.to_parquet(f"{self.gold_base_path}{table_name}.parquet", index=False)
                df.to_sql(table_name, session.get_bind(), if_exists="replace", index=False)
            session.commit()