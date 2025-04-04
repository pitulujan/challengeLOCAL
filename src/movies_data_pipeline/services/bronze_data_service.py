from typing import Dict, Any, List
from fastapi import HTTPException, BackgroundTasks
import pandas as pd
import uuid
import logging
from datetime import datetime
from .etl_service import ETLService  

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

    async def update(self, updates: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, Any]:
        """Update existing entries in the bronze layer by UUID or movie name for single or multiple records with batched Typesense updates."""
        df = self.etl_service.extractor.load_bronze_data()

        # Convert input to list
        if isinstance(updates, dict):
            updates_list = [updates]
        elif isinstance(updates, list):
            updates_list = updates
        else:
            raise HTTPException(status_code=400, detail="Input must be a dict or list of dicts")

        # Collect updates for batch processing
        updated_records = []
        typesense_updates = []
        not_found_identifiers = []

        for update in updates_list:
            if "uuid" not in update and "name" not in update:
                raise HTTPException(status_code=400, detail="Each update must contain either 'uuid' or 'name' as identifier")

            # Filter by UUID or name
            identifier = update.get("uuid") or update.get("name")
            try:
                uuid.UUID(identifier)
                if 'uuid' not in df.columns:
                    raise HTTPException(status_code=500, detail="No 'uuid' column found in bronze data")
                result = df[df["uuid"] == identifier]
            except (ValueError, TypeError):
                if 'name' not in df.columns:
                    raise HTTPException(status_code=500, detail="No 'name' column found in bronze data")
                result = df[df["name"] == identifier]

            if result.empty:
                not_found_identifiers.append((update.get("name"), update.get("uuid")))
                continue  # Skip to the next update if not found

            # Validate update keys (excluding identifier)
            update_data = {k: v for k, v in update.items() if k not in ["uuid", "name"]}
            valid_columns = df.columns.tolist()
            invalid_keys = [key for key in update_data.keys() if key not in valid_columns]
            if invalid_keys:
                raise HTTPException(status_code=400, detail=f"Invalid keys for identifier {identifier}: {invalid_keys}")

            # Update data
            current_time = datetime.now()
            condition = (df["uuid"] == identifier) if 'uuid' in df.columns and identifier in df["uuid"].values else (df["name"] == identifier)
            for key, value in update_data.items():
                df.loc[condition, key] = value
            df.loc[condition, 'updated_at'] = current_time

            # Store updated record and convert Timestamp to string
            updated_record = df[condition].iloc[0].to_dict()
            # Convert any Timestamp objects to ISO format strings
            for key, value in updated_record.items():
                if isinstance(value, pd.Timestamp):
                    updated_record[key] = value.isoformat()
            updated_records.append((updated_record.get("name"), updated_record.get("uuid")))

            # Prepare Typesense update (action: "update" for partial updates)
            typesense_id = identifier if ('uuid' in df.columns and identifier in df["uuid"].values) else updated_record.get('name')
            typesense_updates.append({
                "action": "update",
                "id": typesense_id,
                "doc": updated_record
            })

        # Save all updates to parquet at once if there were any updates
        if updated_records:
            df.to_parquet(self.bronze_path, index=False)

            # Batch update Typesense
            self.etl_service.batch_update_typesense(typesense_updates)

            # Schedule ETL once after all updates
            background_tasks.add_task(self._run_etl)

        # Prepare response
        total_updates = len(updates_list)
        updated_count = len(updated_records)
        not_found_count = len(not_found_identifiers)
        message = f"{updated_count} records updated, {not_found_count} records not found"

        return {
            "message": message,
            "updated_records": updated_records,  # List of (name, uuid) tuples for updated records
            "not_found_records": not_found_identifiers  # List of (name, uuid) tuples for records not found
        }

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