import pandas as pd
import os
from typing import Dict
from movies_data_pipeline.services.search_service import SearchService
from movies_data_pipeline.data_access.database import get_session_direct

class ETLService:
    def __init__(self):
        self.bronze_path = "src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet"
        self.silver_base_path = "src/movies_data_pipeline/data_access/data_lake/silver/"
        self.gold_base_path = "src/movies_data_pipeline/data_access/data_lake/gold/"
        self.search_service = SearchService()

    def extract(self, file_path: str, file_type: str) -> pd.DataFrame:
        """
        Extract data from a source file (CSV, JSON, or PDF) and store as Parquet in the Bronze layer.

        Args:
            file_path (str): Path to the source file.
            file_type (str): Type of the source file ('csv', 'json', or 'pdf').

        Returns:
            pd.DataFrame: The extracted DataFrame.
        """
        # Read data based on file type
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        # elif file_type == "pdf":
        #     with pdfplumber.open(file_path) as pdf:
        #         text = "".join(page.extract_text() for page in pdf.pages)
        #     # Placeholder: Convert text to DataFrame (custom logic needed based on PDF structure)
        #     df = pd.DataFrame({"text": [text]})
        else:
            raise ValueError("Unsupported file type. Use 'csv', 'json', or 'pdf'.")

        # Append to existing Bronze data if it exists
        if os.path.exists(self.bronze_path):
            existing_df = pd.read_parquet(self.bronze_path)
            df = pd.concat([existing_df, df], ignore_index=True)

        # Save to Bronze layer
        df.to_parquet(self.bronze_path, index=False)
        return df

    def transform(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Transform raw data from the Bronze layer into dimension, bridge, and fact tables for the Silver layer,
        and create aggregate tables for the Gold layer.

        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: A dictionary with 'silver' and 'gold' keys, each containing
            a dictionary of DataFrames.
        """
        # Load raw data from Bronze layer
        raw_df = pd.read_parquet(self.bronze_path)

        # Handle date conversion
        raw_df["date_x"] = pd.to_datetime(raw_df["date_x"], format="%m/%d/%Y", errors="coerce")

        # --- Silver Layer: Dimension Tables (no duplicates) ---

        # dim_date: Unique dates
        dim_date = raw_df[["date_x"]].drop_duplicates().reset_index(drop=True)
        dim_date["date_id"] = dim_date.index + 1
        dim_date["year"] = dim_date["date_x"].dt.year
        dim_date["month"] = dim_date["date_x"].dt.month
        dim_date["day"] = dim_date["date_x"].dt.day
        dim_date["quarter"] = dim_date["date_x"].dt.quarter

        # dim_genre: Unique genres
        genres = raw_df["genre"].str.split(",\s+", expand=True).stack().unique()
        dim_genre = pd.DataFrame({"genre_name": genres})
        dim_genre["genre_id"] = dim_genre.index + 1

        # dim_language: Unique languages
        dim_language = raw_df[["orig_lang"]].drop_duplicates().reset_index(drop=True)
        dim_language.columns = ["language_name"]
        dim_language["language_id"] = dim_language.index + 1

        # dim_country: Unique countries
        dim_country = raw_df[["country"]].drop_duplicates().reset_index(drop=True)
        dim_country.columns = ["country_name"]
        dim_country["country_id"] = dim_country.index + 1

        # dim_movie: Link to dimensions
        dim_movie = raw_df.merge(dim_date, on="date_x") \
                          .merge(dim_language, left_on="orig_lang", right_on="language_name") \
                          .merge(dim_country, left_on="country", right_on="country_name")
        dim_movie["movie_id"] = dim_movie.index + 1
        dim_movie = dim_movie[["movie_id", "names", "orig_title", "overview", "status", "crew", "date_id", "language_id", "country_id"]].rename(columns={"names": "name"})

        # --- Silver Layer: Bridge Tables ---

        # bridge_movie_genre: Many-to-many relationship between movies and genres
        movie_genre = raw_df["genre"].str.split(",\s+", expand=True).stack().reset_index(level=1, drop=True).rename("genre_name")
        movie_genre_df = pd.DataFrame({"movie_id": dim_movie["movie_id"].repeat(movie_genre.groupby(level=0).size()), "genre_name": movie_genre})
        bridge_movie_genre = movie_genre_df.merge(dim_genre, on="genre_name")
        bridge_movie_genre["movie_genre_id"] = bridge_movie_genre.index + 1
        bridge_movie_genre = bridge_movie_genre[["movie_genre_id", "movie_id", "genre_id"]]

        # bridge_movie_crew: Assuming crew is a comma-separated string of "Name, Role"
        crew_data = raw_df["crew"].str.split(",\s+", expand=True).stack().reset_index(level=1, drop=True).rename("crew_info")
        crew_df = pd.DataFrame({"movie_id": dim_movie["movie_id"].repeat(crew_data.groupby(level=0).size()), "crew_info": crew_data})
        crew_df[["crew_name", "role"]] = crew_df["crew_info"].str.split(", ", expand=True)

        # dim_crew: Unique crew members
        dim_crew = crew_df[["crew_name"]].drop_duplicates().reset_index(drop=True)
        dim_crew["crew_id"] = dim_crew.index + 1

        # dim_role: Unique roles
        dim_role = crew_df[["role"]].drop_duplicates().reset_index(drop=True)
        dim_role["role_id"] = dim_role.index + 1

        # bridge_movie_crew: Links movie_id, crew_id, role_id, and character_name
        bridge_movie_crew = crew_df.merge(dim_crew, on="crew_name").merge(dim_role, on="role")
        bridge_movie_crew["movie_crew_id"] = bridge_movie_crew.index + 1
        bridge_movie_crew = bridge_movie_crew[["movie_crew_id", "movie_id", "crew_id", "role_id", "crew_info"]].rename(columns={"crew_info": "character_name"})

        # --- Silver Layer: Fact Table ---

        # factMoviePerformance: Include foreign keys to dim_date, dim_language, dim_country
        fact_movie_performance = raw_df.merge(dim_movie, on=["names", "date_x", "orig_title"])
        fact_movie_performance["financial_id"] = fact_movie_performance.index + 1
        fact_movie_performance["profit"] = fact_movie_performance["revenue"] - fact_movie_performance["budget_x"]
        fact_movie_performance = fact_movie_performance[[
            "financial_id", "movie_id", "date_id", "language_id", "country_id",
            "score", "budget_x", "revenue", "profit"
        ]].rename(columns={"budget_x": "budget"})

        # --- Index Movies into Typesense ---
        # Prepare movie data for indexing
        movies_to_index = dim_movie.merge(bridge_movie_genre, on="movie_id") \
                                   .merge(dim_genre, on="genre_id") \
                                   .merge(dim_date, on="date_id") \
                                   .merge(dim_language, on="language_id") \
                                   .merge(dim_country, on="country_id") \
                                   .merge(fact_movie_performance, on="movie_id")

        # Group genres by movie_id
        genres_by_movie = movies_to_index.groupby("movie_id")["genre_name"].apply(list).reset_index()

        # Prepare crew data
        crew_by_movie = bridge_movie_crew.merge(dim_crew, on="crew_id").merge(dim_role, on="role_id")
        crew_by_movie["crew_entry"] = crew_by_movie.apply(
            lambda row: {"name": row["crew_name"], "role_name": row["role"], "character_name": row["character_name"]}, axis=1
        )
        crew_by_movie = crew_by_movie.groupby("movie_id")["crew_entry"].apply(list).reset_index()

        # Merge all data for indexing
        movies_to_index = movies_to_index.drop_duplicates(subset=["movie_id"]).merge(genres_by_movie, on="movie_id").merge(crew_by_movie, on="movie_id")

        # Format movies for Typesense
        for _, row in movies_to_index.iterrows():
            movie_dict = {
                "name": row["name"],
                "orig_title": row["orig_title"],
                "overview": row["overview"],
                "status": row["status"],
                "release_date": row["date_x"].strftime("%Y-%m-%d"),
                "genres": row["genre_name_x"],
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

        # 1. Total revenue by genre
        revenue_by_genre = fact_movie_performance.merge(bridge_movie_genre, on="movie_id") \
                                                 .merge(dim_genre, on="genre_id") \
                                                 .groupby("genre_name")["revenue"].sum().reset_index() \
                                                 .rename(columns={"revenue": "total_revenue"})

        # 2. Average score by year
        avg_score_by_year = fact_movie_performance.merge(dim_movie, on="movie_id") \
                                                  .merge(dim_date, on="date_id") \
                                                  .groupby("year")["score"].mean().reset_index() \
                                                  .rename(columns={"score": "avg_score"})

        # Return all tables in a nested dictionary
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
        # Load Silver layer
        for table_name, df in transformed_data["silver"].items():
            df.to_parquet(f"{self.silver_base_path}{table_name}.parquet", index=False)

        # Load Gold layer
        with get_session_direct() as session:
            for table_name, df in transformed_data["gold"].items():
                # Save to Parquet
                df.to_parquet(f"{self.gold_base_path}{table_name}.parquet", index=False)
                # Save to PostgreSQL using the session
                df.to_sql(table_name, session.get_bind(), if_exists="replace", index=False)
                session.commit()