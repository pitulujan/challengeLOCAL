# app/api/controllers/crud_controller.py
from fastapi import APIRouter, UploadFile, File, BackgroundTasks, Depends, HTTPException, Query
from typing import Dict, List, Union, Any
from movies_data_pipeline.services.bronze_service import BronzeService
from movies_data_pipeline.data_access.database import get_db_engine
from movies_data_pipeline.domain.models.bronze import BronzeMovieUpdate
import logging
from pathlib import Path
import os

logger = logging.getLogger(__name__)

class CrudController:
    def __init__(self):
        """Initialize the CrudController with a router."""
        self.router = APIRouter()
        self._register_routes()

    # Dependency
    def get_bronze_service(self, db_engine=Depends(get_db_engine)) -> BronzeService:
        bronze_file_path = Path(os.getenv("BRONZE_BASE_PATH")) / "bronze_movies.parquet"
        return BronzeService(bronze_file_path)

    def _register_routes(self):
        """Register all CRUD routes for the bronze layer."""

        @self.router.get("/data/", response_model=Dict[str, Any])
        async def get_bronze_paginated(
            page: int = Query(1, ge=1, description="Page number (1-based)"),
            page_size: int = Query(10, ge=1, le=100, description="Number of records per page"),
            bronze_service: BronzeService = Depends(self.get_bronze_service)
        ):
            """Fetch a paginated subset of raw bronze data."""
            try:
                df, page, page_size, total_records, total_pages = bronze_service.get_bronze_paginated(page, page_size)
                return {
                    "data": df.to_dict(orient="records"),
                    "page": page,
                    "page_size": page_size,
                    "total_records": total_records,
                    "total_pages": total_pages
                }
            except Exception as e:
                logger.error(f"Failed to fetch paginated bronze data: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.get("/files/", response_model=Dict[str, List[str]])
        async def list_bronze_files(
            bronze_service: BronzeService = Depends(self.get_bronze_service)
        ):
            """List all files in the bronze directory, excluding bronze_movies.parquet."""
            try:
                files = bronze_service.list_bronze_files()
                return {"files": files}
            except Exception as e:
                logger.error(f"Failed to list bronze files: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
            
        @self.router.put("/data/", response_model=Dict[str, str])
        async def update_bronze_data(
            updates: Union[BronzeMovieUpdate, List[BronzeMovieUpdate]],
            background_tasks: BackgroundTasks,
            bronze_service: BronzeService = Depends(self.get_bronze_service)
        ):
            """Update one or more bronze records with JSON data."""
            update_list = [updates] if isinstance(updates, BronzeMovieUpdate) else updates
            
            try:
                background_tasks.add_task(bronze_service.update_bronze, update_list)
                return {"message": f"Updated {len(update_list)} bronze record(s) and ETL processing started"}
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
            except Exception as e:
                logger.error(f"Failed to update bronze data: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete("/data/{bronze_id}", response_model=Dict[str, str])
        async def delete_bronze_data(
            bronze_id: str,
            background_tasks: BackgroundTasks,
            bronze_service: BronzeService = Depends(self.get_bronze_service)
        ):
            """Delete a specific record from bronze by bronze_id."""
            try:
                background_tasks.add_task(bronze_service.delete_bronze, bronze_id)
                return {"message": f"Record {bronze_id} deleted and ETL processing started"}
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
            except Exception as e:
                logger.error(f"Failed to delete bronze data: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
            
