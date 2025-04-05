from fastapi import APIRouter, BackgroundTasks, Query
from typing import Dict, Any, List
from movies_data_pipeline.services.bronze_data_service import BronzeDataService
import logging
import os
from pathlib import Path

class CrudController:
    def __init__(self):
        self.router = APIRouter()
        self.bronze_service = BronzeDataService(os.getenv("BRONZE_MOVIES_PATH"))
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        async def create_raw(data: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, str]:
            """Create a new record in the Bronze layer."""
            logging.debug(f"Received raw input: {data}")
            return await self.bronze_service.create(data, background_tasks)

        @self.router.get("/{identifier}")
        async def read_raw_v1(identifier: str) -> List[Dict[str, Any]]:
            """Read a record from the Bronze layer by UUID or movie name (v1)."""
            return await self.bronze_service.read(identifier)

        @self.router.get("/get_full_raw/")
        async def read_all_raw_v2(
            page: int = Query(1, ge=1, description="Page number, starting from 1"),
            page_size: int = Query(10, ge=1, le=100, description="Number of records per page, max 100")
        ) -> Dict[str, Any]:
            """Read all records from the Bronze layer with pagination ."""
            df, total_records = self.bronze_service.etl_service.extractor.load_paginated_bronze_data(
                page=page, page_size=page_size, read_only=True
            )
            if df.empty:
                return {
                    "data": [],
                    "page": page,
                    "page_size": page_size,
                    "total_records": 0,
                    "total_pages": 0
                }
            
            total_pages = (total_records + page_size - 1) // page_size  # Ceiling division
            return {
                "data": df.to_dict(orient="records"),
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages
            }

        @self.router.put("/")
        async def update_raw(updates: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, Any]:
            """Update one or multiple records in the Bronze layer by UUID or movie name."""
            result = await self.bronze_service.update(updates, background_tasks)
            return {
                "message": result["message"],
                "updated_records": result["updated_records"]
            }

        @self.router.delete("/")
        async def delete_raw(uuids: str | List[str], background_tasks: BackgroundTasks) -> Dict[str, Any]:
            """Delete one or multiple records from the Bronze layer by UUID."""
            result = await self.bronze_service.delete(uuids, background_tasks)
            return {
                "message": result["message"],
                "deleted_count": result["deleted_count"],
                "not_found": result["not_found"]
            }