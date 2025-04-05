from typing import Dict, Any, List
from fastapi import HTTPException, BackgroundTasks
import pandas as pd
import uuid
import logging
from datetime import datetime
from .etl_service import ETLService  

logger = logging.getLogger(__name__)

class BronzeDataService:
    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path
        self.etl_service = ETLService()  

    def _run_etl(self):
        """Run ETL process."""
        try:
            transformed_data = self.etl_service.transform()
            self.etl_service.load(transformed_data)
            logger.info("ETL process completed")
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")

    async def create(self, data: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, str]:
        """Create new entries in the bronze layer."""
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise HTTPException(status_code=400, detail="Input must be a dict or list")

        mandatory_columns = {"name", "orig_title", "overview", "status", "date_x", "genre", "crew", "country", "orig_lang", "budget_x", "revenue", "score"}
        for item in data_list:
            missing_columns = mandatory_columns - set(item.keys())
            if missing_columns:
                raise HTTPException(status_code=400, detail=f"Missing columns: {missing_columns}")

        new_rows, new_records_count = self.etl_service.extractor.extract_from_dicts(data_list)
        for _, row in new_rows.iterrows():
            self.etl_service.update_typesense("create", row.to_dict())

        if new_records_count > 0:
            background_tasks.add_task(self._run_etl)
            return {"message": f"{new_records_count} new entries added, ETL scheduled"}
        return {"message": f"{len(new_rows)} entries processed, no new data, ETL not scheduled"}

    async def read(self, identifier: str) -> List[Dict[str, Any]]:
        """Read raw data by UUID or name."""
        df = self.etl_service.extractor.load_bronze_data(read_only=True)
        try:
            uuid.UUID(identifier)
            result = df[df["uuid"] == identifier]
        except ValueError:
            if 'name' not in df.columns:
                raise HTTPException(status_code=500, detail="No 'name' column in data")
            result = df[df["name"] == identifier]

        if result.empty:
            raise HTTPException(status_code=404, detail="Movie not found")
        return result.to_dict(orient="records")

    async def update(self, updates: Dict[str, Any] | List[Dict[str, Any]], background_tasks: BackgroundTasks) -> Dict[str, Any]:
        """Update records with UUID regeneration."""
        df = self.etl_service.extractor.load_bronze_data()

        if isinstance(updates, dict):
            updates_list = [updates]
        elif isinstance(updates, list):
            updates_list = updates
        else:
            raise HTTPException(status_code=400, detail="Input must be a dict or list")

        updated_records = []
        typesense_updates = []
        not_found_identifiers = []

        for update in updates_list:
            if "uuid" not in update and "name" not in update:
                raise HTTPException(status_code=400, detail="Must provide 'uuid' or 'name'")

            identifier = update.get("uuid") or update.get("name")
            try:
                uuid.UUID(identifier)
                condition = df["uuid"] == identifier
            except (ValueError, TypeError):
                if 'name' not in df.columns:
                    raise HTTPException(status_code=500, detail="No 'name' column in data")
                condition = df["name"] == identifier

            result = df[condition]
            if result.empty:
                not_found_identifiers.append((update.get("name"), update.get("uuid")))
                continue

            original_record = result.iloc[0].to_dict()
            update_data = {k: v for k, v in update.items() if k not in ["uuid", "name"]}
            valid_columns = df.columns.tolist()
            invalid_keys = [k for k in update_data if k not in valid_columns]
            if invalid_keys:
                raise HTTPException(status_code=400, detail=f"Invalid keys: {invalid_keys}")

            # Check for actual changes
            changed = False
            for key, value in update_data.items():
                if key in original_record and original_record[key] != value:
                    changed = True
                    break

            if changed:
                for key, value in update_data.items():
                    df.loc[condition, key] = value
                df.loc[condition, 'updated_at'] = datetime.now()
                df.loc[condition, 'uuid'] = df[condition].apply(self.etl_service.extractor._generate_canonical_uuid, axis=1)
                updated_record = df[condition].iloc[0].to_dict()
                for key, value in updated_record.items():
                    if isinstance(value, pd.Timestamp):
                        updated_record[key] = value.isoformat()
                updated_records.append((updated_record.get("name"), updated_record.get("uuid")))
                typesense_updates.append({
                    "action": "update",
                    "id": updated_record['uuid'],
                    "doc": updated_record
                })

        if updated_records:
            df.to_parquet(self.bronze_path, index=False)
            self.etl_service.batch_update_typesense(typesense_updates)
            background_tasks.add_task(self._run_etl)

        message = f"{len(updated_records)} updated, {len(not_found_identifiers)} not found"
        return {
            "message": message,
            "updated_records": updated_records,
            "not_found_records": not_found_identifiers
        }

    async def delete(self, movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
        """Delete entries by name."""
        df = self.etl_service.extractor.load_bronze_data()
        if 'name' not in df.columns:
            raise HTTPException(status_code=500, detail="No 'name' column in data")
        if not (df["name"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")

        df = df[df["name"] != movie_name]
        df.to_parquet(self.bronze_path, index=False)
        self.etl_service.update_typesense("delete", {}, movie_name)
        background_tasks.add_task(self._run_etl)
        return {"message": "Data deleted, ETL scheduled"}