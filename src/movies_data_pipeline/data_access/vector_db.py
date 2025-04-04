import typesense
from typing import List, Dict, Any
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class VectorDB:
    def __init__(self, initialize=False):
        self.client = typesense.Client({
            "nodes": [{"host": "typesense", "port": "8108", "protocol": "http"}],
            "api_key": "xyz",
            "connection_timeout_seconds": 10  # Increased timeout for larger batches
        })
        self.collection_name = "movies"
        if initialize:
            self._initialize_collection()

    def _initialize_collection(self) -> None:
        schema = {
            "name": self.collection_name,
            "enable_nested_fields": True,
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "orig_title", "type": "string"},
                {"name": "overview", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "release_date", "type": "string"},
                {"name": "genres", "type": "string[]"},
                {"name": "crew", "type": "object[]"},
                {"name": "country", "type": "string"},
                {"name": "language", "type": "string"},
                {"name": "budget", "type": "float"},
                {"name": "revenue", "type": "float"},
                {"name": "score", "type": "float"},
                {"name": "is_deleted", "type": "bool"}
            ]
        }
        try:
            self.client.collections[self.collection_name].delete()
        except:
            pass
        self.client.collections.create(schema)

    def search_movies(self, query: str, per_page: int = 10, page: int = 1) -> List[Dict[str, Any]]:
        """Search for movies in Typesense and return raw hits."""
        search_params = {
            "q": query,
            "query_by": "name,overview,genres,country,language",
            "per_page": per_page,
            "page": page
        }
        result = self.client.collections[self.collection_name].documents.search(search_params)
        return result["hits"]

    def index_movie(self, movie: Dict[str, Any]) -> None:
        """Index a single movie in Typesense."""
        try:
            if "id" not in movie or not movie["id"]:
                raise ValueError("Movie document must contain a valid 'id' field (UUID)")
            self.client.collections[self.collection_name].documents.upsert(movie)
        except Exception as e:
            logger.error(f"Failed to index movie {movie.get('name', 'unknown')}: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def batch_index_movies(self, movies: List[Dict[str, Any]], batch_size: int = 10000) -> None:
        """Batch index multiple movies in Typesense with retries."""
        try:
            for movie in movies:
                if "id" not in movie or not movie["id"]:
                    raise ValueError(f"Movie document must contain a valid 'id' field (UUID): {movie}")
            # Use coerce_or_drop to handle type mismatches gracefully
            self.client.collections[self.collection_name].documents.import_(
                movies,
                {"action": "upsert", "dirty_values": "coerce_or_drop", "batch_size": batch_size}
            )
            logger.debug(f"Successfully indexed batch of {len(movies)} movies")
        except Exception as e:
            logger.error(f"Failed to batch index movies: {str(e)}")
            raise

    def delete_movie(self, movie_id: str) -> None:
        """Delete a movie from Typesense by its UUID."""
        try:
            self.client.collections[self.collection_name].documents[movie_id].delete()
        except Exception as e:
            logger.error(f"Failed to delete movie with ID {movie_id}: {str(e)}")
            raise