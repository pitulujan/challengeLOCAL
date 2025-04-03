from typing import Dict, Any, List
from fastapi import HTTPException, BackgroundTasks
import pandas as pd
import os
from datetime import datetime
from movies_data_pipeline.services.etl_service import ETLService
import uuid
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class BronzeDataService:
    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path
        self.etl_service = ETLService()

    def _run_etl(self):
        """Helper method to run the full ETL process."""
        try:
            transformed_data = self.etl_service.transform()
            self.etl_service.load(transformed_data)
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")

    async def create(self, data: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, str]:
        current_time = datetime.now()
        data['created_at'] = current_time
        data['updated_at'] = current_time
        df = pd.DataFrame([data])
        if os.path.exists(self.bronze_path):
            existing = pd.read_parquet(self.bronze_path)
            if 'created_at' not in existing.columns:
                existing['created_at'] = pd.NaT
            if 'updated_at' not in existing.columns:
                existing['updated_at'] = pd.NaT
            df = pd.concat([existing, df], ignore_index=True)
        df.to_parquet(self.bronze_path, index=False)
        
        # Update Typesense immediately
        self.etl_service.update_typesense("create", data)
        # Schedule full ETL in the background
        background_tasks.add_task(self._run_etl)
        
        return {"message": "Raw data created, ETL process scheduled"}

    async def read(self, identifier: str) -> List[Dict[str, Any]]:
        """Read a record from the Bronze layer by UUID or movie name."""
        df = pd.read_parquet(self.bronze_path)
        
        # Determine if the identifier is a UUID
        try:
            uuid.UUID(identifier)  # Validate if it's a UUID
            result = df[df["uuid"] == identifier]
        except ValueError:
            # If not a UUID, assume it's a movie name
            # Check for 'name' or 'names' column
            name_col = "name" if "name" in df.columns else "names" if "names" in df.columns else None
            if not name_col:
                raise HTTPException(status_code=500, detail="No 'name' or 'names' column found in bronze data")
            result = df[df[name_col] == identifier]
        
        if result.empty:
            raise HTTPException(status_code=404, detail="Movie not found")
        return result.to_dict(orient="records")

    async def update(self, movie_name: str, data: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
        df = pd.read_parquet(self.bronze_path)
        name_col = "name" if "name" in df.columns else "names" if "names" in df.columns else None
        if not name_col or not (df[name_col] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        
        valid_columns = df.columns.tolist()
        invalid_keys = [key for key in data.keys() if key not in valid_columns and key not in ['created_at', 'updated_at']]
        if invalid_keys:
            raise HTTPException(status_code=400, detail=f"Invalid keys: {invalid_keys}")
        
        current_time = datetime.now()
        for key, value in data.items():
            df.loc[df[name_col] == movie_name, key] = value
        df.loc[df[name_col] == movie_name, 'updated_at'] = current_time
        df.to_parquet(self.bronze_path, index=False)
        
        # Update Typesense immediately with the updated data
        updated_data = df[df[name_col] == movie_name].iloc[0].to_dict()
        self.etl_service.update_typesense("update", updated_data, movie_name)
        # Schedule full ETL in the background
        background_tasks.add_task(self._run_etl)
        
        return updated_data

    async def delete(self, movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
        df = pd.read_parquet(self.bronze_path)
        name_col = "name" if "name" in df.columns else "names" if "names" in df.columns else None
        if not name_col:
            raise KeyError(f"'name' or 'names' column not found in raw data. Available columns: {df.columns.tolist()}")
        if not (df[name_col] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        df = df[df[name_col] != movie_name]
        df.to_parquet(self.bronze_path, index=False)
        
        # Update Typesense immediately to mark as deleted
        self.etl_service.update_typesense("delete", {}, movie_name)
        # Schedule full ETL in the background
        background_tasks.add_task(self._run_etl)
        
        return {"message": "Raw data deleted, ETL process scheduled"}