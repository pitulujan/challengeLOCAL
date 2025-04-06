from typing import Dict, List, Any
from movies_data_pipeline.services.search_service import SearchService
from .transformer_service import Transformer
import logging
import pandas as pd
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

class SearchServiceAdapter:
    def __init__(self, bronze_path: str, db_session=None):
        self.bronze_path = bronze_path
        self.search_service = SearchService(db_session=db_session)
        self.transformer = Transformer(self.bronze_path)
    
    def create_document(self, movie_data: Dict[str, Any]) -> None:
        """Create a search document from raw data."""
        try:
            movie_dict = self._prepare_movie(movie_data)
            self.search_service.index_movie(movie_dict)
        except Exception as e:
            logger.error(f"Failed to create document: {str(e)}")
            raise
    
    def batch_create_documents(self, movie_data_list: List[Dict[str, Any]], batch_size: int = 1000, num_threads: int = 4) -> None:
        """Batch create search documents with parallel processing."""
        processed_movies = self._prepare_movies_bulk(movie_data_list)
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
            raise Exception("Errors in parallel indexing: " + "; ".join(str(e) for e in exceptions))
        
        logger.info(f"Batch indexed {len(processed_movies)} movies with {len(threads)} threads")

    def _index_chunk(self, chunk: List[Dict[str, Any]], batch_size: int, exceptions: List[Exception]) -> None:
        try:
            self.search_service.batch_index_movies(chunk, batch_size=batch_size)
        except Exception as e:
            exceptions.append(e)

    def _prepare_movie(self, movie_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a single movie for Typesense (simplified for gold layer sync)."""
        # Since Typesense syncs with gold, this is a fallback for raw data
        df = pd.DataFrame([movie_data])
        df = self.transformer._standardize_columns(df)
        df = self.transformer._process_dates(df)
        df = self.transformer._process_genre_and_crew(df)
        
        row = df.iloc[0]
        return {
            "id": str(row.get("movie_id", row["bronze_id"])),  # Fallback to bronze_id if no movie_id
            "name": row["name"],
            "orig_title": row.get("orig_title", row["name"]),
            "overview": row.get("overview", ""),
            "status": row.get("status", "Unknown"),
            "genres": row["genre_list"],
            "created_at": row.get("created_at", datetime.now()).isoformat(),
            "updated_at": row.get("updated_at", datetime.now()).isoformat()
        }

    def _prepare_movies_bulk(self, movie_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare movies in bulk."""
        df = pd.DataFrame(movie_data_list)
        df = self.transformer._standardize_columns(df)
        df = self.transformer._process_dates(df)
        df = self.transformer._process_genre_and_crew(df)
        
        movies = []
        for _, row in df.iterrows():
            movies.append({
                "id": str(row.get("movie_id", row["bronze_id"])),
                "name": row["name"],
                "orig_title": row.get("orig_title", row["name"]),
                "overview": row.get("overview", ""),
                "status": row.get("status", "Unknown"),
                "genres": row["genre_list"],
                "created_at": row.get("created_at", datetime.now()).isoformat(),
                "updated_at": row.get("updated_at", datetime.now()).isoformat()
            })
        return movies
    
    def update_document(self, movie_data: Dict[str, Any], movie_id: str) -> None:
        """Update a search document."""
        movie_dict = self._prepare_movie(movie_data)
        movie_dict["id"] = movie_id  # Ensure correct ID
        self.search_service.index_movie(movie_dict)
        logger.info(f"Updated movie '{movie_dict['name']}'")

    def delete_document(self, movie_id: str) -> None:
        """Delete a search document."""
        self.search_service.delete_movie(movie_id)
        logger.info(f"Deleted movie ID {movie_id}")