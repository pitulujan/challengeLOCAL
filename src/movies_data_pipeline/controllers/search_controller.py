from fastapi import APIRouter
from movies_data_pipeline.services.search_service import SearchService
from movies_data_pipeline.domain.models.movie import Movie
from typing import List

class SearchController:
    def __init__(self):
        self.router = APIRouter()
        self.search_service = SearchService()
        self._register_routes()

    def _register_routes(self):
        @self.router.get("/", response_model=List[Movie])
        def search_movies(query: str, limit: int = 10, offset: int = 0) -> List[Movie]:
            """
            Search for movies using Typesense with pagination.

            Args:
                query (str): The search query.
                limit (int): Number of results to return (default: 10).
                offset (int): Offset for pagination (default: 0).

            Returns:
                List[Movie]: List of matching movies.
            """
            movies = self.search_service.search_movies(query, limit, offset)
            return movies

        @self.router.post("/index")
        def index_movies() -> dict:
            """Index movies into Typesense."""
            # Note: Indexing is now handled by ETLService after transformation
            return {"message": "Indexing should be triggered via ETL pipeline"}