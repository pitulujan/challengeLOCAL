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

        # Standardize 'names' to 'name' early
        for item in data_list:
            if 'names' in item and 'name' not in item:
                item['name'] = item.pop('names')
            elif 'names' in item:
                del item['names']

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
        df = self.etl_service.extractor.load_bronze_data(read_only=True)

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
            if "uuid" not in update:
                raise HTTPException(status_code=400, detail="Must provide 'uuid' for update")

            identifier = update["uuid"]
            try:
                uuid.UUID(identifier)
                condition = df["uuid"] == identifier
            except (ValueError, TypeError):
                raise HTTPException(status_code=400, detail="Invalid UUID format")

            result = df[condition]
            if result.empty:
                not_found_identifiers.append((update.get("name"), identifier))
                continue

            original_record = result.iloc[0].to_dict()
            update_data = {k: v for k, v in update.items() if k != "uuid"}
            
            # Standardize 'names' to 'name'
            if 'names' in update_data and 'name' not in update_data:
                update_data['name'] = update_data.pop('names')
            elif 'names' in update_data:
                del update_data['names']

            valid_columns = df.columns.tolist()
            invalid_keys = [k for k in update_data if k not in valid_columns]
            if invalid_keys:
                raise HTTPException(status_code=400, detail=f"Invalid keys: {invalid_keys}")

            # Check if UUID-affecting fields changed
            uuid_affecting_fields = ['name', 'orig_title']
            uuid_changed = False
            for key in uuid_affecting_fields:
                if key in update_data and key in original_record and original_record[key] != update_data[key]:
                    uuid_changed = True
                    break

            # Apply updates
            changed = False
            for key, value in update_data.items():
                if key in original_record and original_record[key] != value:
                    df.loc[condition, key] = value
                    changed = True

            if changed:
                # Update Typesense with original UUID first
                original_uuid = original_record['uuid']
                df.loc[condition, 'updated_at'] = datetime.now()
                typesense_doc = self._prepare_typesense_doc(df[condition].iloc[0].to_dict())
                typesense_doc["id"] = original_uuid
                typesense_updates.append(typesense_doc)

                # Regenerate UUID if necessary
                if uuid_changed:
                    new_uuid = self.etl_service.extractor._generate_canonical_uuid(df[condition].iloc[0])
                    df.loc[condition, 'uuid'] = new_uuid

                updated_record = df[condition].iloc[0].to_dict()
                for key, value in updated_record.items():
                    if isinstance(value, pd.Timestamp):
                        updated_record[key] = value.isoformat()
                updated_records.append((updated_record.get("name"), updated_record.get("uuid")))

        if updated_records:
            df.to_parquet(self.bronze_path, index=False)
            try:
                self.etl_service.batch_update_typesense(typesense_updates)
                logger.info(f"Successfully sent {len(typesense_updates)} updates to Typesense")
            except Exception as e:
                logger.error(f"Failed to update Typesense: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Typesense update failed: {str(e)}")
            background_tasks.add_task(self._run_etl)

        message = f"{len(updated_records)} updated, {len(not_found_identifiers)} not found"
        return {
            "message": message,
            "updated_records": updated_records,
            "not_found_records": not_found_identifiers
        }

    def _prepare_typesense_doc(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a record for Typesense indexing with proper crew parsing."""
        release_date = record.get("date_x")
        if isinstance(release_date, pd.Timestamp):
            release_date = release_date.strftime("%Y-%m-%d") if pd.notna(release_date) else "Unknown"
        elif isinstance(release_date, str):
            try:
                parsed_date = pd.to_datetime(release_date, errors='coerce')
                release_date = parsed_date.strftime("%Y-%m-%d") if pd.notna(parsed_date) else "Unknown"
            except ValueError:
                release_date = "Unknown"
        else:
            release_date = "Unknown"

        # Parse crew into actor_name and character_name pairs
        crew_str = record.get("crew", "")
        crew_list = []
        if crew_str:
            crew_items = [item.strip() for item in crew_str.split(", ") if item.strip()]
            # Assuming crew alternates between actor_name and character_name
            for i in range(0, len(crew_items) - 1, 2):
                actor_name = crew_items[i]
                character_name = crew_items[i + 1] if i + 1 < len(crew_items) else "Unknown"
                crew_list.append({"actor_name": actor_name, "character_name": character_name})

        return {
            "id": record.get("uuid", ""),
            "name": record.get("name", ""),
            "orig_title": record.get("orig_title", record.get("name", "")),
            "overview": record.get("overview", ""),
            "status": record.get("status", "Unknown"),
            "release_date": release_date,
            "genres": record.get("genre", "").split(", ") if record.get("genre") else [],
            "crew": crew_list,
            "country": record.get("country", ""),
            "language": record.get("orig_lang", ""),
            "budget": float(record.get("budget_x", 0)),
            "revenue": float(record.get("revenue", 0)),
            "score": float(record.get("score", 0)),
            "is_deleted": False
        }

    async def delete(self, movie_name: str, background_tasks: BackgroundTasks) -> Dict[str, str]:
        """Delete entries by name."""
        df = self.etl_service.extractor.load_bronze_data(read_only=True)
        if 'name' not in df.columns:
            raise HTTPException(status_code=500, detail="No 'name' column in data")
        if not (df["name"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")

        df = df[df["name"] != movie_name]
        df.to_parquet(self.bronze_path, index=False)
        self.etl_service.update_typesense("delete", {}, movie_name)
        background_tasks.add_task(self._run_etl)
        return {"message": "Data deleted, ETL scheduled"}