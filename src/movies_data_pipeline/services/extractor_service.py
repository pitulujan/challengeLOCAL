import pandas as pd
import os
import uuid
import json
from typing import Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Extractor:
    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path
    
    def load_bronze_data(self, read_only: bool = False) -> pd.DataFrame:
        if os.path.exists(self.bronze_path):
            df = pd.read_parquet(self.bronze_path)
            if read_only:
                return df
            df = self._standardize_columns(df)
            df = self._process_chunk(df)
            df['uuid'] = df.apply(self._generate_canonical_uuid, axis=1)
            return df
        return pd.DataFrame()

    def extract(self, file_path: str, batch_size: int = 1000) -> Tuple[pd.DataFrame, int]:
        file_type = file_path.split(".")[-1].lower()
        full_df = pd.DataFrame()
        existing_df = self.load_bronze_data(read_only=True)
        new_records_count = 0

        if file_type == "json":
            with open(file_path, 'r') as f:
                data = json.load(f)
            df_new = pd.DataFrame(data if isinstance(data, list) else [data])
        elif file_type == "csv":
            df_new = pd.read_csv(file_path)
        else:
            raise ValueError("Unsupported file type. Use 'csv' or 'json'.")

        # Standardize columns and add metadata
        df_new = self._standardize_columns(df_new)
        df_new = self._process_chunk(df_new)
        df_new['uuid'] = df_new.apply(self._generate_canonical_uuid, axis=1)

        if not existing_df.empty:
            # Check for new or changed records
            new_records = df_new[~df_new['uuid'].isin(existing_df['uuid'])]
            if not new_records.empty:
                full_df = pd.concat([existing_df, new_records], ignore_index=True)
                new_records_count = len(new_records)
            else:
                full_df = existing_df  # No new records
        else:
            full_df = df_new
            new_records_count = len(df_new)

        # Save to bronze layer
        if new_records_count > 0:
            full_df.to_parquet(self.bronze_path, index=False)
            logger.info(f"Extracted {len(full_df)} records from {file_path}, {new_records_count} new")
        else:
            logger.debug(f"No new records to process from {file_path}")

        return full_df, new_records_count

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'names' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'names': 'name'})
        elif 'names' in df.columns:
            df = df.drop(columns=['names'])
        return df

    def _process_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        if "genre" in df.columns:
            df["genre"] = df["genre"].astype(str).replace("nan", "").str.strip()
        if "crew" in df.columns:
            df["crew"] = df["crew"].astype(str).replace("nan", "").str.strip()

        numeric_columns = ['score', 'budget_x', 'revenue']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        string_columns = ['name', 'orig_title', 'orig_lang', 'status', 'overview', 'country']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        return self._add_metadata(df)

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        current_time = pd.Timestamp.now()
        if 'uuid' not in df.columns:
            df['uuid'] = df.apply(self._generate_canonical_uuid, axis=1)
        if 'created_at' not in df.columns:
            df['created_at'] = current_time
        df['updated_at'] = current_time
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        return df

    def _generate_canonical_uuid(self, row: pd.Series) -> str:
        name = row['name'].strip() if pd.notna(row['name']) else ""
        orig_title = row['orig_title'].strip() if 'orig_title' in row and pd.notna(row['orig_title']) else ""
        namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
        canonical_str = f"{name}|{orig_title}"
        return str(uuid.uuid5(namespace, canonical_str))