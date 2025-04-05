from fastapi import APIRouter, BackgroundTasks
from typing import Dict, Any, List
from movies_data_pipeline.services.bronze_data_service import BronzeDataService
import logging

class CrudController:
    def __init__(self):
        self.router = APIRouter()
        self.bronze_service = BronzeDataService("src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet")
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/v1")
        async def create_raw(data: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, str]:
            """Create a new record in the Bronze layer."""
            logging.debug(f"Received raw input: {data}")
            return await self.bronze_service.create(data, background_tasks)

        @self.router.get("/v1/{identifier}")
        async def read_raw_v1(identifier: str) -> List[Dict[str, Any]]:
            """Read a record from the Bronze layer by UUID or movie name (v1)."""
            return await self.bronze_service.read(identifier)

        @self.router.get("/v2")
        async def read_all_raw_v2() -> List[Dict[str, Any]]:
            """Read all records from the Bronze layer (v2)."""
            df = self.bronze_service.etl_service.extractor.load_bronze_data(read_only=True)
            if df.empty:
                return []
            return df.to_dict(orient="records")

        @self.router.put("/v1")
        async def update_raw(updates: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, Any]:
            """Update one or multiple records in the Bronze layer by UUID or movie name."""
            result = await self.bronze_service.update(updates, background_tasks)
            return {
                "message": result["message"],
                "updated_records": result["updated_records"]
            }

        @self.router.delete("/v1/{movie_name}")
        async def delete_raw(movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
            """Delete a record from the Bronze layer by movie_name."""
            return await self.bronze_service.delete(movie_name, background_tasks)