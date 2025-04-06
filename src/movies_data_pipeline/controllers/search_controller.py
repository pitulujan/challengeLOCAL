from fastapi import APIRouter
from movies_data_pipeline.services.search_service import SearchService
from movies_data_pipeline.domain.models.movie import Movie
from typing import List

class SearchController:
    def __init__(self, db_session=None):
        self.router = APIRouter()
        self.search_service = SearchService(db_session=db_session)
        self._register_routes()

    def _register_routes(self):
        @self.router.get("/", response_model=List[Movie])
        def search_movies(
            query: str,
            genre: str = None,
            limit: int = 10,
            offset: int = 0
        ) -> List[Movie]:
            """
            Search movies with optional genre filter and pagination.

            Args:
                query (str): Search query.
                genre (str, optional): Filter by genre.
                limit (int): Results per page (default: 10).
                offset (int): Pagination offset (default: 0).

            Returns:
                List[Movie]: Matching movies.
            """
            return self.search_service.search_movies(query, limit, offset, genre)