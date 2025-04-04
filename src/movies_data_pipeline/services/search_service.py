from typing import List, Dict, Any
from datetime import datetime
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.domain.models.movie import Movie

class SearchService:
    def __init__(self):
        self.vector_db = VectorDB(initialize=False)

    def search_movies(self, query: str, limit: int = 10, offset: int = 0) -> List[Movie]:
        """Search movies in Typesense by query with pagination."""
        page = (offset // limit) + 1
        per_page = limit
        hits = self.vector_db.search_movies(query, per_page=per_page, page=page)

        movies = []
        for hit in hits:
            doc = hit['document']
            release_date_str = doc['release_date']
            try:
                release_date = datetime.strptime(release_date_str, "%Y-%m-%d").date() if release_date_str != "Unknown" else None
            except ValueError:
                release_date = None

            movie = Movie(
                name=doc['name'],
                orig_title=doc.get('orig_title', doc['name']),
                overview=doc['overview'],
                status=doc.get('status', 'Unknown'),
                release_date=release_date,
                genres=doc['genres'],
                crew=doc.get('crew', []),
                country=doc['country'],
                language=doc['language'],
                budget=doc.get('budget', 0.0),
                revenue=doc.get('revenue', 0.0),
                score=doc['score'],
                is_deleted=doc.get('is_deleted', False)
            )
            movies.append(movie)
        return movies

    def index_movie(self, movie: Dict[str, Any]) -> None:
        """Index or update a single movie in Typesense."""
        self.vector_db.index_movie(movie)

    def batch_index_movies(self, movies: List[Dict[str, Any]], batch_size: int = 10000) -> None:
        """Batch index or update multiple movies in Typesense."""
        self.vector_db.batch_index_movies(movies, batch_size=batch_size)

    def delete_movie(self, movie_id: str) -> None:
        """Delete a movie from Typesense by its UUID."""
        self.vector_db.delete_movie(movie_id)

    def clear_index(self) -> None:
        """Clear the Typesense index and reinitialize it."""
        self.vector_db._initialize_collection()