import typesense
from typing import List, Dict, Any
import logging
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.orm import Session
from movies_data_pipeline.data_access.models.gold import DimMovie

logger = logging.getLogger(__name__)

class VectorDB:
    def __init__(self, initialize: bool = False, db_session: Session = None):
        """Initialize Typesense client using environment variables and optionally sync with gold layer.
        
        Args:
            initialize: If True, initialize the Typesense collection.
            db_session: SQLAlchemy session for syncing with gold layer data.
        """
        api_key = os.getenv("TYPESENSE_API_KEY")
        if not api_key:
            raise ValueError("TYPESENSE_API_KEY environment variable not set")
        
        self.client = typesense.Client({
            "nodes": [{"host": "typesense", "port": "8108", "protocol": "http"}],
            "api_key": api_key,
            "connection_timeout_seconds": 10
        })
        self.collection_name = "movies"
        self.db_session = db_session
        
        self.data_dir = os.getenv("TYPESENSE_DATA_DIR", "/data")
        logger.debug(f"Typesense data directory set to: {self.data_dir}")
        
        if initialize and db_session:
            self._initialize_collection()
            self._sync_with_gold()

    def _initialize_collection(self) -> None:
        """Initialize Typesense collection for DimMovie data."""
        schema = {
            "name": self.collection_name,
            "enable_nested_fields": True,
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "orig_title", "type": "string"},
                {"name": "overview", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "genres", "type": "string[]", "optional": True},
                {"name": "created_at", "type": "string", "optional": True},
                {"name": "updated_at", "type": "string", "optional": True}
            ]
        }
        try:
            self.client.collections[self.collection_name].delete()
            logger.info(f"Deleted existing Typesense collection: {self.collection_name}")
        except Exception as e:
            logger.debug(f"No existing collection to delete or deletion failed: {str(e)}")
        
        self.client.collections.create(schema)
        logger.info(f"Created Typesense collection: {self.collection_name}")

    def _sync_with_gold(self) -> None:
        """Sync Typesense with DimMovie from gold layer."""
        if not self.db_session:
            logger.error("Database session required for syncing with gold layer")
            return
        
        try:
            movies = self.db_session.query(DimMovie).all()
            documents = [
                {
                    "id": str(movie.movie_id),
                    "name": movie.name,
                    "orig_title": movie.orig_title,
                    "overview": movie.overview,
                    "status": movie.status,
                    "created_at": movie.created_at.isoformat(),
                    "updated_at": movie.updated_at.isoformat()
                } for movie in movies
            ]
            
            if not documents:
                logger.info("No movies found in gold layer (DimMovie); skipping Typesense sync")
                return
            
            self.batch_index_movies(documents)
            logger.info(f"Synced {len(documents)} movies from gold layer to Typesense")
        except Exception as e:
            logger.error(f"Failed to sync with gold layer: {str(e)}")
            raise

    def search_movies(self, query: str, per_page: int = 10, page: int = 1) -> List[Dict[str, Any]]:
        """Search for movies in Typesense."""
        search_params = {
            "q": query,
            "query_by": "name,orig_title,overview",
            "per_page": per_page,
            "page": page
        }
        try:
            result = self.client.collections[self.collection_name].documents.search(search_params)
            return result["hits"]
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise

    def index_movie(self, movie: Dict[str, Any]) -> None:
        """Index a single movie in Typesense."""
        try:
            if "id" not in movie or not movie["id"]:
                raise ValueError("Movie document must contain a valid 'id' field")
            self.client.collections[self.collection_name].documents.upsert(movie)
            logger.debug(f"Indexed movie: {movie.get('name', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to index movie {movie.get('name', 'unknown')}: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def batch_index_movies(self, movies: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Batch index movies in Typesense with retries.
        
        Args:
            movies: List of movie dictionaries
            batch_size: Number of documents per batch
        """
        try:
            if not movies:
                logger.warning("Attempted to batch index an empty list of movies; skipping")
                return
            
            for movie in movies:
                if "id" not in movie or not movie["id"]:
                    raise ValueError(f"Movie document must contain a valid 'id' field: {movie}")
            
            self.client.collections[self.collection_name].documents.import_(
                movies,
                {"action": "upsert", "dirty_values": "coerce_or_drop", "batch_size": batch_size}
            )
            logger.debug(f"Successfully indexed batch of {len(movies)} movies")
        except Exception as e:
            logger.error(f"Failed to batch index movies: {str(e)}")
            raise

    def delete_movie(self, movie_id: str) -> None:
        """Delete a movie from Typesense by movie_id."""
        try:
            self.client.collections[self.collection_name].documents[movie_id].delete()
            logger.debug(f"Deleted movie with ID {movie_id}")
        except Exception as e:
            logger.error(f"Failed to delete movie with ID {movie_id}: {str(e)}")
            raise