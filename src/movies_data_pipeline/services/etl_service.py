import pandas as pd
import os
from typing import Dict, Any
from fastapi import UploadFile, BackgroundTasks
from datetime import datetime
import uuid
from .extractor_service import Extractor
from .transformer_service import Transformer
from .loader_service import Loader
from .search_service_adapter import SearchServiceAdapter
from movies_data_pipeline.services.search_service import SearchService
from movies_data_pipeline.data_access.database import get_session_direct
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ETLService:
    def __init__(self, bronze_path: str = None, silver_base_path: str = None, gold_base_path: str = None):
        """Initialize the ETL Service with paths and component classes."""
        self.bronze_path = bronze_path or "src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet"
        self.silver_base_path = silver_base_path or "src/movies_data_pipeline/data_access/data_lake/silver/"
        self.gold_base_path = gold_base_path or "src/movies_data_pipeline/data_access/data_lake/gold/"
        
        # Initialize components
        self.extractor = Extractor(self.bronze_path)
        self.transformer = Transformer(self.bronze_path)
        self.loader = Loader(self.silver_base_path, self.gold_base_path)
        self.search_adapter = SearchServiceAdapter(self.bronze_path)
        self.search_service = SearchService()

    def extract(self, file: UploadFile, background_tasks: BackgroundTasks = None) -> pd.DataFrame:
        file_type = file.filename.split(".")[-1].lower()
        if file_type == "csv":
            df = pd.read_csv(file.file)
        elif file_type == "json":
            df = pd.read_json(file.file)
        else:
            raise ValueError("Unsupported file type. Use 'csv', 'json', or 'pdf'.")
        
        # Normalize 'names' to 'name' at ingestion
        if 'names' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'names': 'name'})
        elif 'names' in df.columns:
            df = df.drop(columns=['names'])
        
        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time
        
        def generate_uuid(row):
            raw_data = row.drop(['created_at', 'updated_at', 'uuid'], errors='ignore').astype(str).to_dict()
            data_str = ''.join(f"{k}:{v}" for k, v in sorted(raw_data.items()))
            namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
            return str(uuid.uuid5(namespace, data_str))
        
        if 'uuid' not in df.columns:
            df['uuid'] = df.apply(generate_uuid, axis=1)
        
        if os.path.exists(self.bronze_path):
            existing_df = pd.read_parquet(self.bronze_path)
            if 'names' in existing_df.columns and 'name' not in existing_df.columns:
                existing_df = existing_df.rename(columns={'names': 'name'})
            elif 'names' in existing_df.columns:
                existing_df = existing_df.drop(columns=['names'])
            if 'created_at' not in existing_df.columns:
                existing_df['created_at'] = pd.NaT
            if 'updated_at' not in existing_df.columns:
                existing_df['updated_at'] = pd.NaT
            if 'uuid' not in existing_df.columns:
                existing_df['uuid'] = existing_df.apply(generate_uuid, axis=1)
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['uuid'], keep='last')
        
        df.to_parquet(self.bronze_path, index=False)
        
        for _, row in df.iterrows():
            self.update_typesense("create", row.to_dict())
        
        if background_tasks:
            background_tasks.add_task(self._run_full_etl)
        
        return df

    def _run_full_etl(self):
        try:
            transformed_data = self.transform()
            self.load(transformed_data)
            logger.info("Full ETL process completed")
        except Exception as e:
            logger.error(f"Full ETL process failed: {str(e)}")

    def transform(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        raw_df = pd.read_parquet(self.bronze_path)
        logger.debug("Sample raw date_x: %s", raw_df["date_x"].head(5).tolist())

        # Normalize 'names' to 'name'
        if 'names' in raw_df.columns and 'name' not in raw_df.columns:
            raw_df = raw_df.rename(columns={'names': 'name'})
        elif 'names' in raw_df.columns:
            raw_df = raw_df.drop(columns=['names'])
        
        if 'name' not in raw_df.columns:
            raise KeyError("Input data must contain a 'name' column for movie titles.")

        possible_date_cols = ["date_x", "release_date", "date"]
        date_col = next((col for col in possible_date_cols if col in raw_df.columns), None)
        if date_col:
            raw_df = raw_df.rename(columns={date_col: "date_x"})
            raw_df["date_x"] = raw_df["date_x"].str.strip()
            raw_df["date_x"] = pd.to_datetime(raw_df["date_x"], format="%m/%d/%Y", errors="coerce")
            logger.debug("Parsed date_x: %s", raw_df["date_x"].head(5).tolist())
        else:
            raise KeyError("Input data must contain a date column ('date_x', 'release_date', or 'date')")

        raw_df["genre_list"] = raw_df["genre"].str.split(",\s+")

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

        fact_movie_performance = raw_df.merge(dim_movie, on=["name", "date_x", "orig_title"])
        fact_movie_performance["financial_id"] = fact_movie_performance.index + 1
        fact_movie_performance["profit"] = fact_movie_performance["revenue"] - fact_movie_performance["budget_x"]
        fact_movie_performance = fact_movie_performance[[
            "financial_id", "movie_id", "date_id", "language_id", "country_id",
            "score", "budget_x", "revenue", "profit"
        ]].rename(columns={"budget_x": "budget"})

        revenue_by_genre = fact_movie_performance.merge(bridge_movie_genre, on="movie_id") \
                                                .merge(dim_genre, on="genre_id") \
                                                .groupby("genre_name")["revenue"].sum().reset_index() \
                                                .rename(columns={"revenue": "total_revenue"})

        merged_df = fact_movie_performance.merge(dim_movie, on="movie_id", suffixes=('_fact', '_movie'))
        avg_score_by_year = merged_df.merge(dim_date, left_on="date_id_movie", right_on="date_id", suffixes=('_movie', '_date')) \
                                    .groupby("year")["score"].mean().reset_index() \
                                    .rename(columns={"score": "avg_score"})

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
        for table_name, df in transformed_data["silver"].items():
            df.to_parquet(f"{self.silver_base_path}{table_name}.parquet", index=False)

        with get_session_direct() as session:
            for table_name, df in transformed_data["gold"].items():
                df.to_parquet(f"{self.gold_base_path}{table_name}.parquet", index=False)
                df.to_sql(table_name, session.get_bind(), if_exists="replace", index=False)
            session.commit()

    def update_typesense(self, operation: str, movie_data: Dict[str, Any], movie_name: str = None) -> None:
        if operation == "delete":
            if not movie_name:
                raise ValueError("movie_name is required for delete operation")
            df = pd.read_parquet(self.bronze_path)
            if 'names' in df.columns and 'name' not in df.columns:
                df = df.rename(columns={'names': 'name'})
            elif 'names' in df.columns:
                df = df.drop(columns=['names'])
            movie_row = df[df["name"] == movie_name]
            if movie_row.empty:
                logger.warning(f"Movie '{movie_name}' not found in bronze layer for deletion")
                return
            uuid_to_delete = movie_row.iloc[0]["uuid"]
            self.search_service.delete_movie(uuid_to_delete)
            logger.info(f"Deleted movie '{movie_name}' with UUID '{uuid_to_delete}' from Typesense")
            return

        raw_df = pd.DataFrame([movie_data])
        if "names" in raw_df.columns and "name" not in raw_df.columns:
            raw_df = raw_df.rename(columns={"names": "name"})
        elif "names" in raw_df.columns:
            raw_df = raw_df.drop(columns=["names"])
        
        if "uuid" not in raw_df.columns and movie_name:
            df = pd.read_parquet(self.bronze_path)
            if 'names' in df.columns and 'name' not in df.columns:
                df = df.rename(columns={'names': 'name'})
            elif 'names' in df.columns:
                df = df.drop(columns=['names'])
            movie_row = df[df["name"] == movie_name]
            if not movie_row.empty:
                raw_df["uuid"] = movie_row.iloc[0]["uuid"]
            else:
                logger.warning(f"Movie '{movie_name}' not found; generating new UUID")
                raw_df["uuid"] = str(uuid.uuid4())
        
        possible_date_cols = ["date_x", "release_date", "date"]
        date_col = next((col for col in possible_date_cols if col in raw_df.columns), None)
        if date_col:
            raw_df["date_x"] = pd.to_datetime(raw_df[date_col], format="%m/%d/%Y", errors="coerce")
        else:
            raw_df["date_x"] = pd.NaT

        raw_df["genre_list"] = raw_df["genre"].str.split(",\s+") if "genre" in raw_df.columns else [[]]
        def parse_crew(crew_str):
            if pd.isna(crew_str) or crew_str == "":
                return []
            crew_list = crew_str.split(", ")
            pairs = []
            for i in range(0, len(crew_list), 2):
                if i + 1 < len(crew_list):
                    pairs.append({"name": crew_list[i], "character_name": crew_list[i + 1]})
                else:
                    pairs.append({"name": crew_list[i], "character_name": "Self"})
            return pairs
        raw_df["crew"] = raw_df["crew"].apply(parse_crew) if "crew" in raw_df.columns else [[]]

        row = raw_df.iloc[0]
        release_date = row["date_x"].strftime("%Y-%m-%d") if pd.notna(row["date_x"]) else "Unknown"
        movie_dict = {
            "id": row["uuid"],
            "name": row["name"],
            "orig_title": row.get("orig_title", row["name"]),
            "overview": row.get("overview", ""),
            "status": row.get("status", "Unknown"),
            "release_date": release_date,
            "genres": row["genre_list"],
            "crew": row["crew"],
            "country": row.get("country", ""),
            "language": row.get("orig_lang", ""),
            "budget": float(row.get("budget_x", 0)),
            "revenue": float(row.get("revenue", 0)),
            "score": float(row.get("score", 0)),
            "is_deleted": False
        }
        self.search_service.index_movie(movie_dict)
        logger.info(f"Updated Typesense with {operation} for movie '{row['name']}' with UUID '{row['uuid']}'")