import typesense
from typing import List, Dict, Any

class VectorDB:
    def __init__(self):
        self.client = typesense.Client({
            "nodes": [{"host": "typesense", "port": "8108", "protocol": "http"}],
            "api_key": "xyz",
            "connection_timeout_seconds": 2
        })
        self.collection_name = "movies"
        self._initialize_collection()

    def _initialize_collection(self) -> None:
        """Initialize the Typesense collection for movies."""
        schema = {
            "name": self.collection_name,
            "enable_nested_fields": True,
            "fields": [
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
            'query_by': 'name,overview,genres,country,language',
            "per_page": per_page,
            "page": page
        }
        result = self.client.collections[self.collection_name].documents.search(search_params)
        return result["hits"]

    def index_movie(self, movie: Dict[str, Any]) -> None:
        """
        Index a single movie in Typesense.

        Args:
            movie (Dict[str, Any]): Movie data to index.
        """
        self.client.collections[self.collection_name].documents.create(movie)