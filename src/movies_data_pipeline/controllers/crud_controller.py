from fastapi import APIRouter, BackgroundTasks, HTTPException
from typing import Dict, Any, List, Tuple
from movies_data_pipeline.services.bronze_data_service import BronzeDataService

class CrudController:
    def __init__(self):
        self.router = APIRouter()
        self.bronze_service = BronzeDataService("src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet")
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        async def create_raw(data: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, str]:
            """Create a new record in the Bronze layer."""
            return await self.bronze_service.create(data, background_tasks)

        @self.router.get("/{identifier}")
        async def read_raw(identifier: str) -> List[Dict[str, Any]]:
            """Read a record from the Bronze layer by UUID or movie name."""
            return await self.bronze_service.read(identifier)

        @self.router.put("/")
        async def update_raw(updates: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, Any]:
            """Update one or multiple records in the Bronze layer by UUID or movie name."""
            result = await self.bronze_service.update(updates, background_tasks)
            return {
                "message": result["message"],
                "updated_records": result["updated_records"]
            }

        @self.router.delete("/{movie_name}")
        async def delete_raw(movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
            """Delete a record from the Bronze layer by movie_name."""
            return await self.bronze_service.delete(movie_name, background_tasks)