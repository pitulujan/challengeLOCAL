from typing import List, Dict, Any
from datetime import date
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.domain.models.movie import Movie

class SearchService:
    def __init__(self, db_session=None):
        self.vector_db = VectorDB(initialize=False, db_session=db_session)

    def search_movies(self, query: str, limit: int = 10, offset: int = 0, genre: str = None) -> List[Movie]:
        page = (offset // limit) + 1
        hits = self.vector_db.search_movies(query, per_page=limit, page=page, genre_filter=genre)
        
        movies = []
        for hit in hits:
            doc = hit["document"]
            release_date_str = doc.get("release_date")
            try:
                release_date = date.fromisoformat(release_date_str.split("T")[0]) if release_date_str else None
            except (ValueError, TypeError):
                release_date = None

            movies.append(Movie(
                name=doc["name"],
                orig_title=doc.get("orig_title", doc["name"]),
                overview=doc["overview"],
                status=doc.get("status", "Unknown"),
                release_date=release_date,
                genres=doc.get("genres", []),
                crew=doc.get("crew", []),
                language=doc.get("language"),
            ))
        return movies