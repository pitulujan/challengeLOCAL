import pandas as pd
import os
import uuid
import json
from typing import Tuple, List, Dict, Any
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

    def load_paginated_bronze_data(self, page: int, page_size: int, read_only: bool = False) -> Tuple[pd.DataFrame, int]:
        """Load a paginated subset of the bronze data."""
        if os.path.exists(self.bronze_path):
            df = pd.read_parquet(self.bronze_path)
            total_records = len(df)
            
            # Calculate start and end indices for pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            # Slice the DataFrame
            paginated_df = df.iloc[start_idx:end_idx]
            
            if read_only:
                return paginated_df, total_records
            paginated_df = self._standardize_columns(paginated_df)
            paginated_df = self._process_chunk(paginated_df)
            paginated_df['uuid'] = paginated_df.apply(self._generate_canonical_uuid, axis=1)
            return paginated_df, total_records
        return pd.DataFrame(), 0

    def extract(self, file_path: str, batch_size: int = 1000) -> Tuple[pd.DataFrame, int]:
        """Extract data from a file (JSON or CSV) and integrate with existing bronze data."""
        file_type = file_path.split(".")[-1].lower()
        full_df = pd.DataFrame()
        existing_df = self.load_bronze_data(read_only=True)
        new_records_count = 0

        if file_type == "json":
            with open(file_path, 'r') as f:
                data = json.load(f)
            df_new = pd.DataFrame(data if isinstance(data, list) else [data])
            df_new = self._standardize_columns(df_new)
            df_new = self._process_chunk(df_new)
            df_new['uuid'] = df_new.apply(self._generate_canonical_uuid, axis=1)

            if not existing_df.empty:
                new_records = df_new[~df_new['uuid'].isin(existing_df['uuid'])]
                if not new_records.empty:
                    full_df = pd.concat([existing_df, new_records], ignore_index=True)
                    new_records_count = len(new_records)
                else:
                    full_df = existing_df
            else:
                full_df = df_new
                new_records_count = len(df_new)

        elif file_type == "csv":
            for chunk in pd.read_csv(file_path, chunksize=batch_size):
                df_new = self._standardize_columns(chunk)
                df_new = self._process_chunk(df_new)
                df_new['uuid'] = df_new.apply(self._generate_canonical_uuid, axis=1)

                if not existing_df.empty:
                    new_records = df_new[~df_new['uuid'].isin(existing_df['uuid'])]
                    if not new_records.empty:
                        full_df = pd.concat([full_df, new_records], ignore_index=True)
                        new_records_count += len(new_records)
                    existing_df = pd.concat([existing_df, new_records], ignore_index=True) if not new_records.empty else existing_df
                else:
                    full_df = pd.concat([full_df, df_new], ignore_index=True)
                    new_records_count += len(df_new)
                    existing_df = full_df

        else:
            raise ValueError("Unsupported file type. Use 'csv' or 'json'.")

        # Save to bronze layer if there are new records
        if new_records_count > 0:
            full_df.to_parquet(self.bronze_path, index=False)
            logger.info(f"Extracted {len(full_df)} records from {file_path}, {new_records_count} new")
        else:
            logger.debug(f"No new records to process from {file_path}")

        return full_df, new_records_count

    def extract_from_dicts(self, data_list: List[Dict[str, Any]], batch_size: int = 1000) -> Tuple[pd.DataFrame, int]:
        """Extract data from a list of dictionaries and integrate with existing bronze data."""
        full_new_df = pd.DataFrame()
        new_records_count = 0
        existing_df = self.load_bronze_data(read_only=True)

        # Convert input data to DataFrame
        df_new = pd.DataFrame(data_list)
        df_new = self._standardize_columns(df_new)
        df_new = self._process_chunk(df_new)
        df_new['uuid'] = df_new.apply(self._generate_canonical_uuid, axis=1)

        if not existing_df.empty:
            # Filter out records that already exist based on UUID
            new_records = df_new[~df_new['uuid'].isin(existing_df['uuid'])]
            if not new_records.empty:
                full_new_df = pd.concat([existing_df, new_records], ignore_index=True)
                new_records_count = len(new_records)
            else:
                full_new_df = existing_df  # No new records
        else:
            full_new_df = df_new
            new_records_count = len(df_new)

        # Save to bronze layer if there are new records
        if new_records_count > 0:
            full_new_df.to_parquet(self.bronze_path, index=False)
            logger.info(f"Processed {len(full_new_df)} records from dicts, {new_records_count} new")
        else:
            logger.debug("No new records to process from dicts")

        return full_new_df, new_records_count

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