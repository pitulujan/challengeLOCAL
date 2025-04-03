from fastapi import APIRouter
from typing import Dict, Any, List
from movies_data_pipeline.services.bronze_data_service import BronzeDataService

class CrudController:
    def __init__(self):
        self.router = APIRouter()
        self.bronze_service = BronzeDataService("src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet")
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        def create_raw(data: Dict[str, Any]) -> Dict[str, str]:
            """Create a new record in the Bronze layer."""
            return self.bronze_service.create(data)

        @self.router.get("/{movie_name}")
        def read_raw(movie_name: str) -> List[Dict[str, Any]]:
            """Read a record from the Bronze layer by movie_name."""
            return self.bronze_service.read(movie_name)

        @self.router.put("/{movie_name}")
        def update_raw(movie_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
            """Update a record in the Bronze layer by movie_name."""
            return self.bronze_service.update(movie_name, data)

        @self.router.delete("/{movie_name}")
        def delete_raw(movie_name: str) -> Dict[str, str]:
            """Delete a record from the Bronze layer by movie_name."""
            return self.bronze_service.delete(movie_name)