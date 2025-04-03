from typing import Dict, Any, List
from fastapi import HTTPException, BackgroundTasks
import pandas as pd
import os
from datetime import datetime
from movies_data_pipeline.services.etl_service import ETLService  

class BronzeDataService:
    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path
        self.etl_service = ETLService()  

    def _run_etl(self):
        """Helper method to run the ETL process."""
        transformed_data = self.etl_service.transform()
        self.etl_service.load(transformed_data)

    async def create(self, data: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, str]:
        """Create a new record in the Bronze layer with timestamps and trigger ETL in the background."""
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
        
        # Schedule ETL to run in the background
        background_tasks.add_task(self._run_etl)
        
        return {"message": "Raw data created, ETL process scheduled"}

    async def read(self, movie_name: str) -> List[Dict[str, Any]]:
        """Read a record from the Bronze layer by movie_name (no ETL needed)."""
        df = pd.read_parquet(self.bronze_path)
        result = df[df["names"] == movie_name]
        if result.empty:
            raise HTTPException(status_code=404, detail="Movie not found")
        return result.to_dict(orient="records")

    async def update(self, movie_name: str, data: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
        """Update a record in the Bronze layer by movie_name, update timestamp, and trigger ETL in the background."""
        df = pd.read_parquet(self.bronze_path)
        if not (df["names"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        
        # Validate that the keys in data match existing columns
        valid_columns = df.columns.tolist()
        invalid_keys = [key for key in data.keys() if key not in valid_columns and key not in ['created_at', 'updated_at']]
        if invalid_keys:
            raise HTTPException(status_code=400, detail=f"Invalid keys: {invalid_keys}")
        
        # Update the record and updated_at timestamp
        current_time = datetime.now()
        for key, value in data.items():
            df.loc[df["names"] == movie_name, key] = value
        df.loc[df["names"] == movie_name, 'updated_at'] = current_time
        df.to_parquet(self.bronze_path, index=False)
        
        # Schedule ETL to run in the background
        background_tasks.add_task(self._run_etl)
        
        # Return the updated record
        updated_record = df[df["names"] == movie_name].iloc[0].to_dict()
        return updated_record

    async def delete(self, movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
        """Delete a record from the Bronze layer by movie_name and trigger ETL in the background."""
        df = pd.read_parquet(self.bronze_path)
        if "names" not in df.columns:
            raise KeyError(f"'names' column not found in raw data. Available columns: {df.columns.tolist()}")
        if not (df["names"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        df = df[df["names"] != movie_name]
        df.to_parquet(self.bronze_path, index=False)
        
        # Schedule ETL to run in the background
        background_tasks.add_task(self._run_etl)
        
        return {"message": "Raw data deleted, ETL process scheduled"}