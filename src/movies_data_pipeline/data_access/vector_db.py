import typesense
from typing import List, Dict, Any

class VectorDB:
    def __init__(self, initialize=False):
        self.client = typesense.Client({
            "nodes": [{"host": "typesense", "port": "8108", "protocol": "http"}],
            "api_key": "xyz",
            "connection_timeout_seconds": 2
        })
        self.collection_name = "movies"
        if initialize:
            self._initialize_collection()

    def _initialize_collection(self) -> None:
        schema = {
            "name": self.collection_name,
            "enable_nested_fields": True,
            "fields": [
                {"name": "id", "type": "string"},  # Add UUID as the document ID
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
        """
        Search for movies in Typesense.
        """
        search_params = {
            "q": query,
            "query_by": "name,overview,genres,country,language",
            "per_page": per_page,
            "page": page
        }
        result = self.client.collections[self.collection_name].documents.search(search_params)
        return result["hits"]

    def index_movie(self, movie: Dict[str, Any]) -> None:
        """
        Index a movie in Typesense, using the 'id' field as the document identifier.
        """
        try:
            if "id" not in movie or not movie["id"]:
                raise ValueError("Movie document must contain a valid 'id' field (UUID)")
            # Use upsert to update if the document already exists, or create if it doesn't
            self.client.collections[self.collection_name].documents.upsert(movie)
        except Exception as e:
            print(f"Failed to index movie {movie.get('name', 'unknown')}: {e}")

    def delete_movie(self, movie_id: str) -> None:
        """
        Delete a movie from Typesense by its UUID.
        """
        try:
            self.client.collections[self.collection_name].documents[movie_id].delete()
        except Exception as e:
            print(f"Failed to delete movie with ID {movie_id}: {e}")