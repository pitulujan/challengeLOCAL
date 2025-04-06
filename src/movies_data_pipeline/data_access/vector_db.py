import typesense
from typing import List, Dict, Any
import logging
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.orm import Session
from movies_data_pipeline.data_access.models.gold import (
    DimMovie, BridgeMovieGenre, DimGenre, DimCrew, BridgeMovieCrew, FactMovieMetrics, DimDate, DimLanguage
)

logger = logging.getLogger(__name__)

class VectorDB:
    def __init__(self, initialize: bool = False, db_session: Session = None):
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
        schema = {
            "name": self.collection_name,
            "enable_nested_fields": True,
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "orig_title", "type": "string"},
                {"name": "overview", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "release_date", "type": "string", "optional": True},
                {"name": "genres", "type": "string[]", "optional": True},
                {"name": "crew", "type": "object[]", "optional": True},
                {"name": "created_at", "type": "string", "optional": True},
                {"name": "updated_at", "type": "string", "optional": True}
            ]
        }
        try:
            self.client.collections[self.collection_name].delete()
            logger.info(f"Deleted existing Typesense collection: {self.collection_name}")
        except Exception:
            logger.debug("No existing collection to delete")
        
        self.client.collections.create(schema)
        logger.info(f"Created Typesense collection: {self.collection_name}")

    def _sync_with_gold(self) -> None:
        if not self.db_session:
            logger.error("Database session required for syncing with gold layer")
            return
        
        try:
            # Implemented sync for genres, crew, release_date, and language from the gold layer.
            # Additional fields (e.g., country, budget, revenue) can be implemented following the same
            # pattern by joining with relevant tables (e.g., DimCountry, FactMovieMetrics) and
            # adding them to the Typesense document.
            movies = self.db_session.query(DimMovie).all()
            documents = []
            for movie in movies:
                genres = (
                    self.db_session.query(DimGenre.genre_name)
                    .join(BridgeMovieGenre, BridgeMovieGenre.genre_id == DimGenre.genre_id)
                    .filter(BridgeMovieGenre.movie_id == movie.movie_id)
                    .all()
                )
                genre_names = [g.genre_name for g in genres]

                crew = (
                    self.db_session.query(DimCrew.actor_name, DimCrew.character_name)
                    .join(BridgeMovieCrew, BridgeMovieCrew.crew_id == DimCrew.crew_id)
                    .filter(BridgeMovieCrew.movie_id == movie.movie_id)
                    .all()
                )
                crew_list = [{"actor_name": c.actor_name, "character_name": c.character_name} for c in crew]

                metrics = (
                    self.db_session.query(DimDate.release_date,DimLanguage.language_name)
                    .join(FactMovieMetrics, FactMovieMetrics.date_id == DimDate.date_id)
                    .join(DimLanguage, FactMovieMetrics.language_id == DimLanguage.language_id)
                    .filter(FactMovieMetrics.movie_id == movie.movie_id)
                    .first()
                )
                # Treat release_date as a string directly
                release_date = metrics.release_date if metrics and metrics.release_date else None
                if release_date and " " in release_date:
                    release_date = release_date.split(" ")[0]  # e.g., "2023-03-02"

                language = metrics.language_name if metrics and metrics.language_name else None

                documents.append({
                    "id": str(movie.movie_id),
                    "name": movie.name,
                    "orig_title": movie.orig_title,
                    "overview": movie.overview,
                    "status": movie.status,
                    "release_date": release_date,
                    "genres": genre_names,
                    "crew": crew_list,
                    "language":language,
                    "created_at": movie.created_at.isoformat(),
                    "updated_at": movie.updated_at.isoformat()
                })
            
            if not documents:
                logger.info("No movies found in gold layer; skipping sync")
                return
            
            self.batch_index_movies(documents)
            logger.info(f"Synced {len(documents)} movies to Typesense")
        except Exception as e:
            logger.error(f"Failed to sync with gold layer: {str(e)}")
            raise

    def search_movies(self, query: str, per_page: int = 10, page: int = 1, genre_filter: str = None) -> List[Dict[str, Any]]:
        search_params = {
            "q": query,
            "query_by": "name,orig_title,overview",
            "per_page": per_page,
            "page": page
        }
        if genre_filter:
            search_params["filter_by"] = f"genres:{genre_filter}"
        
        try:
            result = self.client.collections[self.collection_name].documents.search(search_params)
            return result["hits"]
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise

    def index_movie(self, movie: Dict[str, Any]) -> None:
        try:
            if "id" not in movie or not movie["id"]:
                raise ValueError("Movie document must contain a valid 'id'")
            self.client.collections[self.collection_name].documents.upsert(movie)
            logger.debug(f"Indexed movie: {movie.get('name', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to index movie: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def batch_index_movies(self, movies: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        if not movies:
            logger.warning("Empty movie list; skipping batch index")
            return
        
        for movie in movies:
            if "id" not in movie or not movie["id"]:
                raise ValueError(f"Movie must have 'id': {movie}")
        
        self.client.collections[self.collection_name].documents.import_(
            movies,
            {"action": "upsert", "dirty_values": "coerce_or_drop", "batch_size": batch_size}
        )
        logger.debug(f"Indexed {len(movies)} movies")

    def delete_movie(self, movie_id: str) -> None:
        try:
            self.client.collections[self.collection_name].documents[movie_id].delete()
            logger.debug(f"Deleted movie ID {movie_id}")
        except Exception as e:
            logger.error(f"Failed to delete movie ID {movie_id}: {str(e)}")
            raise