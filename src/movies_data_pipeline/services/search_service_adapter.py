from typing import Dict, Any, List
from movies_data_pipeline.services.search_service import SearchService
from .transformer_service import Transformer
import logging
import uuid
import pandas as pd
import threading

logger = logging.getLogger(__name__)

class SearchServiceAdapter:
    def __init__(self, bronze_path: str):
        """Initialize the SearchServiceAdapter."""
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
    
    def batch_create_documents(self, movie_data_list: List[Dict[str, Any]], batch_size: int = 10000, num_threads: int = 4) -> None:
        """Batch create search documents for multiple movies with parallel imports."""
        try:
            # Preprocess all movies in bulk
            processed_movies = self._prepare_movies_bulk(movie_data_list)
            
            # Split into chunks for parallel processing
            chunk_size = max(1, len(processed_movies) // num_threads)
            chunks = [processed_movies[i:i + chunk_size] for i in range(0, len(processed_movies), chunk_size)]
            
            threads = []
            exceptions = []
            for chunk in chunks:
                thread = threading.Thread(target=self._index_chunk, args=(chunk, batch_size, exceptions))
                threads.append(thread)
                thread.start()
            
            for thread in threads:
                thread.join()
            
            if exceptions:
                raise Exception("Errors occurred during parallel indexing: " + "; ".join(str(e) for e in exceptions))
            
            logger.info(f"Batch created {len(processed_movies)} search documents using {len(threads)} threads")
        except Exception as e:
            logger.error(f"Failed to batch create search documents: {str(e)}")
            raise
    
    def _index_chunk(self, chunk: List[Dict[str, Any]], batch_size: int, exceptions: List[Exception]) -> None:
        """Index a chunk of movies in Typesense."""
        try:
            self.search_service.batch_index_movies(chunk, batch_size=batch_size)
        except Exception as e:
            exceptions.append(e)

    def _prepare_movies_bulk(self, movie_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare movies in bulk using vectorized operations."""
        df = pd.DataFrame(movie_data_list)
        df = self.transformer._standardize_columns(df)
        df = self.transformer._process_dates(df)
        df = self.transformer._process_genre_and_crew(df)
        
        # Assign UUIDs in bulk
        if "uuid" not in df.columns:
            df["uuid"] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Convert to list of dictionaries efficiently
        processed_movies = []
        for _, row in df.iterrows():
            release_date = row["date_x"].strftime("%Y-%m-%d") if pd.notna(row["date_x"]) else "Unknown"
            movie_dict = {
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
            processed_movies.append(movie_dict)
        return processed_movies
    
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
    
    def _update_typesense(self, operation: str, movie_data: Dict[str, Any], movie_name: str = None) -> None:
        """Update Typesense index with movie data."""
        if operation == "delete":
            if not movie_name:
                raise ValueError("movie_name (UUID) is required for delete operation")
            # Assume movie_name is the UUID for deletion
            try:
                self.search_service.delete_movie(movie_name)
                logger.info(f"Deleted movie with UUID '{movie_name}' from Typesense")
            except Exception as e:
                logger.error(f"Failed to delete movie with UUID '{movie_name}' from Typesense: {str(e)}")
                raise
            return
        
        # Use bulk preparation for create/update consistency
        movie_dict = self._prepare_movies_bulk([movie_data])[0]
        self.search_service.index_movie(movie_dict)
        logger.info(f"Updated Typesense with {operation} for movie '{movie_dict['name']}' with UUID '{movie_dict['id']}'")
        
        # Use bulk preparation for consistency
        movie_dict = self._prepare_movies_bulk([movie_data])[0]
        self.search_service.index_movie(movie_dict)
        logger.info(f"Updated Typesense with {operation} for movie '{movie_dict['name']}' with UUID '{movie_dict['id']}'")