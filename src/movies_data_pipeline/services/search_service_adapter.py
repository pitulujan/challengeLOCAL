from typing import Dict, Any, List
from movies_data_pipeline.services.search_service import SearchService
from .transformer_service import Transformer
import logging
import uuid
import pandas as pd

logger = logging.getLogger(__name__)

class SearchServiceAdapter:
    def __init__(self, bronze_path: str):
        """Initialize the SearchServiceAdapter.
        
        Args:
            bronze_path: Path to the bronze layer data
        """
        self.bronze_path = bronze_path
        self.search_service = SearchService()
        self.transformer = Transformer(self.bronze_path)
    
    def create_document(self, movie_data: Dict[str, Any]) -> None:
        """Create a single search document for a movie."""
        try:
            self._update_typesense("create", movie_data)
        except Exception as e:
            logger.error(f"Failed to create search document: {str(e)}")
            raise
    
    def batch_create_documents(self, movie_data_list: List[Dict[str, Any]]) -> None:
        """Batch create search documents for multiple movies."""
        try:
            processed_movies = []
            for movie_data in movie_data_list:
                processed_movie = self._prepare_movie_dict(movie_data)
                processed_movies.append(processed_movie)
            self.search_service.batch_index_movies(processed_movies)
            logger.info(f"Batch created {len(processed_movies)} search documents")
        except Exception as e:
            logger.error(f"Failed to batch create search documents: {str(e)}")
            raise
    
    def update_document(self, movie_data: Dict[str, Any], movie_name: str) -> None:
        """Update a search document for a movie."""
        try:
            self._update_typesense("update", movie_data, movie_name)
            logger.info(f"Updated search document for movie '{movie_name}'")
        except Exception as e:
            logger.error(f"Failed to update search document: {str(e)}")
            raise
    
    def delete_document(self, movie_name: str) -> None:
        """Delete a search document for a movie."""
        try:
            self._update_typesense("delete", {}, movie_name)
            logger.info(f"Deleted search document for movie '{movie_name}'")
        except Exception as e:
            logger.error(f"Failed to delete search document: {str(e)}")
            raise
    
    def _prepare_movie_dict(self, movie_data: Dict[str, Any], movie_name: str = None) -> Dict[str, Any]:
        """Prepare a movie dictionary for indexing."""
        raw_df = pd.DataFrame([movie_data])
        raw_df = self.transformer._standardize_columns(raw_df)
        raw_df = self.transformer._process_dates(raw_df)
        raw_df = self.transformer._process_genre_and_crew(raw_df)
        
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
                raw_df["uuid"] = str(uuid.uuid4())
        elif "uuid" not in raw_df.columns:
            raw_df["uuid"] = str(uuid.uuid4())
        
        row = raw_df.iloc[0]
        release_date = row["date_x"].strftime("%Y-%m-%d") if pd.notna(row["date_x"]) else "Unknown"
        return {
            "id": row["uuid"],
            "name": row["name"],
            "orig_title": row.get("orig_title", row["name"]),
            "overview": row.get("overview", ""),
            "status": row.get("status", "Unknown"),
            "release_date": release_date,
            "genres": row["genre_list"],
            "crew": row["crew_pairs"],
            "country": row.get("country", ""),
            "language": row.get("orig_lang", ""),
            "budget": float(row.get("budget_x", 0)),
            "revenue": float(row.get("revenue", 0)),
            "score": float(row.get("score", 0)),
            "is_deleted": False
        }
    
    def _update_typesense(self, operation: str, movie_data: Dict[str, Any], movie_name: str = None) -> None:
        """Update Typesense index with movie data."""
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
            return
        
        movie_dict = self._prepare_movie_dict(movie_data, movie_name)
        self.search_service.index_movie(movie_dict)
        logger.info(f"Updated Typesense with {operation} for movie '{movie_dict['name']}' with UUID '{movie_dict['id']}'")