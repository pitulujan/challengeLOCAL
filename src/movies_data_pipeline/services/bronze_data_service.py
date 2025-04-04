from typing import Dict, Any, List
from fastapi import HTTPException, BackgroundTasks
import pandas as pd
import uuid
import logging
from datetime import datetime

# Import the new ETLService (adjust path based on your structure)
from .etl_service import ETLService  # Assuming it's in the same directory; adjust as needed

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class BronzeDataService:
    def __init__(self, bronze_path: str):
            """Initialize the BronzeDataService with the bronze layer path and ETLService."""
            self.bronze_path = bronze_path
            self.etl_service = ETLService()

    def _run_etl(self):
        """Run the full ETL process in the background using ETLService."""
        try:
            transformed_data = self.etl_service.transform()
            self.etl_service.load(transformed_data)
            logger.info("ETL process completed")
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")

    async def create(self, data: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, str]:
        """Create new entries in the bronze layer."""
        # Convert input to list
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise HTTPException(status_code=400, detail="Input must be a dict or list of dicts")

        # Validate mandatory columns
        mandatory_columns = {"name", "orig_title", "overview", "status", "date_x", "genre", "crew", "country", "orig_lang", "budget_x", "revenue", "score"}
        for item in data_list:
            missing_columns = mandatory_columns - set(item.keys())
            if missing_columns:
                raise HTTPException(status_code=400, detail=f"Missing mandatory columns: {missing_columns}")

        # Process data with Extractor via ETLService
        new_rows = self.etl_service.extractor.extract_from_dicts(data_list)

        # Update Typesense for new entries
        for _, row in new_rows.iterrows():
            self.etl_service.update_typesense("create", row.to_dict())

        # Schedule ETL
        background_tasks.add_task(self._run_etl)

        return {"message": f"{len(new_rows)} raw data entries created, ETL process scheduled"}

    async def read(self, identifier: str) -> List[Dict[str, Any]]:
        """Read data from the bronze layer by UUID or movie name."""
        df = self.etl_service.extractor.load_bronze_data()

        # Filter by UUID or name
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
        """Update existing entries in the bronze layer."""
        df = self.etl_service.extractor.load_bronze_data()

        if 'name' not in df.columns or not (df["name"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")

        # Validate update keys
        valid_columns = df.columns.tolist()
        invalid_keys = [key for key in data.keys() if key not in valid_columns]
        if invalid_keys:
            raise HTTPException(status_code=400, detail=f"Invalid keys: {invalid_keys}")

        # Update data
        current_time = datetime.now()
        for key, value in data.items():
            df.loc[df["name"] == movie_name, key] = value
        df.loc[df["name"] == movie_name, 'updated_at'] = current_time

        # Save updated data
        df.to_parquet(self.bronze_path, index=False)

        # Update Typesense
        updated_data = df[df["name"] == movie_name].iloc[0].to_dict()
        self.etl_service.update_typesense("update", updated_data, movie_name)

        # Schedule ETL
        background_tasks.add_task(self._run_etl)

        return updated_data

    async def delete(self, movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
        """Delete entries from the bronze layer."""
        df = self.etl_service.extractor.load_bronze_data()

        if 'name' not in df.columns:
            raise KeyError(f"'name' column not found in raw data. Available columns: {df.columns.tolist()}")
        if not (df["name"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")

        # Remove entries
        df = df[df["name"] != movie_name]
        df.to_parquet(self.bronze_path, index=False)

        # Update Typesense
        self.etl_service.update_typesense("delete", {}, movie_name)

        # Schedule ETL
        background_tasks.add_task(self._run_etl)

        return {"message": "Raw data deleted, ETL process scheduled"}