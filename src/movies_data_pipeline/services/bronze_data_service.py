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

    async def create(self, data: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, str]:
        current_time = datetime.now()
        
        # Convert single dict to list for uniform processing
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise HTTPException(status_code=400, detail="Input must be a dict or list of dicts")

        # Define mandatory columns (using 'name' as standard)
        mandatory_columns = {"name", "orig_title", "overview", "status", "date_x", "genre", "crew", "country", "orig_lang", "budget_x", "revenue", "score"}
        
        # Normalize and validate each entry
        for item in data_list:
            # Prefer 'name', convert 'names' if present
            if "names" in item and "name" not in item:
                item["name"] = item.pop("names")
            elif "names" in item:
                del item["names"]  # Remove 'names' if 'name' exists
            
            missing_columns = mandatory_columns - set(item.keys())
            if missing_columns:
                raise HTTPException(status_code=400, detail=f"Missing mandatory columns: {missing_columns}")
            
            item['created_at'] = current_time
            item['updated_at'] = current_time

        # Convert to DataFrame
        df = pd.DataFrame(data_list)
        
        # Append to existing data, normalizing 'names' to 'name'
        if os.path.exists(self.bronze_path):
            existing = pd.read_parquet(self.bronze_path)
            if 'names' in existing.columns and 'name' not in existing.columns:
                existing = existing.rename(columns={'names': 'name'})
            elif 'names' in existing.columns:
                existing = existing.drop(columns=['names'])
            if 'created_at' not in existing.columns:
                existing['created_at'] = pd.NaT
            if 'updated_at' not in existing.columns:
                existing['updated_at'] = pd.NaT
            df = pd.concat([existing, df], ignore_index=True)
        
        # Generate UUIDs immediately for consistency
        def generate_uuid(row):
            raw_data = row.drop(['created_at', 'updated_at', 'uuid'], errors='ignore').astype(str).to_dict()
            data_str = ''.join(f"{k}:{v}" for k, v in sorted(raw_data.items()))
            namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
            return str(uuid.uuid5(namespace, data_str))
        
        df['uuid'] = df.apply(generate_uuid, axis=1)
        df = df.drop_duplicates(subset=['uuid'], keep='last')
        
        df.to_parquet(self.bronze_path, index=False)
        
        # Update Typesense for new entries
        for _, row in df.tail(len(data_list)).iterrows():
            self.etl_service.update_typesense("create", row.to_dict())
        
        background_tasks.add_task(self._run_etl)
        
        return {"message": f"{len(data_list)} raw data entries created, ETL process scheduled"}

    async def read(self, identifier: str) -> List[Dict[str, Any]]:
        df = pd.read_parquet(self.bronze_path)
        
        # Normalize existing data
        if 'names' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'names': 'name'})
        elif 'names' in df.columns:
            df = df.drop(columns=['names'])
        
        try:
            uuid.UUID(identifier)
            result = df[df["uuid"] == identifier]
        except ValueError:
            if 'name' not in df.columns:
                raise HTTPException(status_code=500, detail="No 'name' column found in bronze data")
            result = df[df["name"] == identifier]
        
        if result.empty:
            raise HTTPException(status_code=404, detail="Movie not found")
        return result.to_dict(orient="records")

    async def update(self, movie_name: str, data: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
        df = pd.read_parquet(self.bronze_path)
        
        if 'names' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'names': 'name'})
        elif 'names' in df.columns:
            df = df.drop(columns=['names'])
        
        if 'name' not in df.columns or not (df["name"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        
        valid_columns = df.columns.tolist()
        invalid_keys = [key for key in data.keys() if key not in valid_columns and key not in ['created_at', 'updated_at']]
        if invalid_keys:
            raise HTTPException(status_code=400, detail=f"Invalid keys: {invalid_keys}")
        
        current_time = datetime.now()
        for key, value in data.items():
            df.loc[df["name"] == movie_name, key] = value
        df.loc[df["name"] == movie_name, 'updated_at'] = current_time
        df.to_parquet(self.bronze_path, index=False)
        
        updated_data = df[df["name"] == movie_name].iloc[0].to_dict()
        self.etl_service.update_typesense("update", updated_data, movie_name)
        background_tasks.add_task(self._run_etl)
        
        return updated_data

    async def delete(self, movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
        df = pd.read_parquet(self.bronze_path)
        
        if 'names' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'names': 'name'})
        elif 'names' in df.columns:
            df = df.drop(columns=['names'])
        
        if 'name' not in df.columns:
            raise KeyError(f"'name' column not found in raw data. Available columns: {df.columns.tolist()}")
        if not (df["name"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        
        df = df[df["name"] != movie_name]
        df.to_parquet(self.bronze_path, index=False)
        
        self.etl_service.update_typesense("delete", {}, movie_name)
        background_tasks.add_task(self._run_etl)
        
        return {"message": "Raw data deleted, ETL process scheduled"}