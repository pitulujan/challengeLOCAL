from typing import List
from datetime import datetime
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.domain.models.movie import Movie

class SearchService:
    def __init__(self):
        self.vector_db = VectorDB()

    def search_movies(self, query: str, limit: int = 10, offset: int = 0) -> List[Movie]:
        """
        Search movies in Typesense by query.
        Supports pagination with limit and offset.
        """
        page = (offset // limit) + 1
        per_page = limit
        hits = self.vector_db.search_movies(query, per_page=per_page, page=page)

        movies = []
        for hit in hits:
            doc = hit['document']
            release_date = datetime.strptime(doc['release_date'], "%Y-%m-%d").date()
            movie = Movie(
                name=doc['name'],
                orig_title=doc.get('orig_title', doc['name']),  # Fallback to name if orig_title not provided
                overview=doc['overview'],
                status=doc.get('status', 'Unknown'),  # Fallback if status not provided
                release_date=release_date,
                genres=doc['genres'],
                crew=doc.get('crew', []),  # Fallback to empty list if crew not provided
                country=doc['country'],
                language=doc['language'],
                budget=doc.get('budget', 0.0),  # Fallback if budget not provided
                revenue=doc.get('revenue', 0.0),  # Fallback if revenue not provided
                score=doc['score'],
                is_deleted=False  # Typesense only indexes non-deleted movies
            )
            movies.append(movie)
        return movies

    def index_movie(self, movie: dict):
        """Index a movie in Typesense."""
        self.vector_db.index_movie(movie)