import pandas as pd
import os
import uuid
import json
import re
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)

class Extractor:
    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path
    
    def load_bronze_data(self, read_only: bool = False) -> pd.DataFrame:
        """Load data from the bronze layer, optionally without modification."""
        if os.path.exists(self.bronze_path):
            df = pd.read_parquet(self.bronze_path)
            if read_only:
                return df  # Return raw data as-is
            df = self._standardize_columns(df)
            df = self._process_chunk(df)
            df['uuid'] = df.apply(self._generate_canonical_uuid, axis=1)
            return df
        return pd.DataFrame()

    def extract(self, file_path: str, batch_size: int = 1000) -> pd.DataFrame:
        """Extract data from a file and save to bronze layer with duplicate handling."""
        file_type = file_path.split(".")[-1].lower()
        full_df = pd.DataFrame()
        existing_df = self.load_bronze_data()

        if file_type == "csv":
            for chunk in pd.read_csv(file_path, chunksize=batch_size):
                df_new = self._standardize_columns(chunk)
                df_new = self._process_chunk(df_new)
                df_new['uuid'] = df_new.apply(self._generate_canonical_uuid, axis=1)
                combined_df = self._merge_and_save(existing_df, df_new)
                full_df = pd.concat([full_df, df_new], ignore_index=True)
                existing_df = combined_df
        elif file_type == "json":
            with open(file_path, 'r') as f:
                data = json.load(f)
            df_new = pd.DataFrame(data if isinstance(data, list) else [data])
            df_new = self._standardize_columns(df_new)
            df_new = self._process_chunk(df_new)
            df_new['uuid'] = df_new.apply(self._generate_canonical_uuid, axis=1)
            combined_df = self._merge_and_save(existing_df, df_new)
            full_df = df_new
            existing_df = combined_df
        else:
            raise ValueError("Unsupported file type. Use 'csv' or 'json'.")

        logger.info(f"Extracted and saved {len(full_df)} records from {file_path}")

        return full_df

    def extract_from_dicts(self, data_list: List[Dict[str, Any]], batch_size: int = 1000) -> Tuple[pd.DataFrame, int]:
        """Extract data from dictionaries, save to bronze, and track new records."""
        standardized_data = []
        for item in data_list:
            std_item = item.copy()
            if 'names' in std_item and 'name' not in std_item:
                std_item['name'] = std_item.pop('names')
            elif 'names' in std_item and 'name' in std_item:
                del std_item['names']
            standardized_data.append(std_item)

        full_new_df = pd.DataFrame()
        new_records_count = 0
        existing_df = self.load_bronze_data()

        for i in range(0, len(standardized_data), batch_size):
            batch = standardized_data[i:i + batch_size]
            df_new = pd.DataFrame(batch)
            df_new = self._standardize_columns(df_new)
            df_new = self._process_chunk(df_new)
            df_new['uuid'] = df_new.apply(self._generate_canonical_uuid, axis=1)

            # Count new records (UUIDs not in existing data)
            existing_uuids = set(existing_df['uuid'])
            new_uuids = set(df_new['uuid']) - existing_uuids
            new_records_count += len(new_uuids)

            combined_df = self._merge_and_save(existing_df, df_new)
            new_or_updated_uuids = set(df_new['uuid'])
            new_rows = combined_df[combined_df['uuid'].isin(new_or_updated_uuids)]
            full_new_df = pd.concat([full_new_df, new_rows], ignore_index=True)
            existing_df = combined_df

        logger.info(f"Processed {len(full_new_df)} records, {new_records_count} new")
        return full_new_df, new_records_count

    def _merge_and_save(self, existing_df: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
        """Merge new data with existing, handle duplicates, and save."""
        combined_df = pd.concat([existing_df, df_new], ignore_index=True)
        duplicated_uuids = combined_df[combined_df.duplicated(subset=['uuid'], keep=False)]['uuid'].unique()
        current_time = pd.Timestamp.now()

        if len(duplicated_uuids) > 0:
            combined_df = combined_df.sort_values('created_at')
            combined_df.loc[combined_df['uuid'].isin(duplicated_uuids), 'updated_at'] = current_time
            combined_df = combined_df.drop_duplicates(subset=['uuid'], keep='first')
        else:
            combined_df['updated_at'] = combined_df['updated_at'].fillna(current_time)

        if 'name' in combined_df.columns:
            combined_df = combined_df.sort_values('name')
        combined_df.to_parquet(self.bronze_path, index=False)
        return combined_df

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names."""
        if 'names' in df.columns:
            if 'name' not in df.columns:
                df = df.rename(columns={'names': 'name'})
            else:
                df = df.drop(columns=['names'])
        return df

    def _process_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process data chunk and add metadata."""
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
        """Add metadata fields."""
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
        """Generate a deterministic UUID."""
        exclude_fields = ['created_at', 'updated_at', 'uuid', 'names']
        canonical_data = {}
        for key in sorted(row.index):
            if key not in exclude_fields:
                value = row[key]
                if pd.isna(value):
                    canonical_data[key] = ""
                elif isinstance(value, (int, float)):
                    canonical_data[key] = str(value).rstrip('0').rstrip('.') if '.' in str(value) else str(value)
                elif isinstance(value, str):
                    canonical_data[key] = re.sub(r'\s+', ' ', value.strip())
                else:
                    canonical_data[key] = str(value)
        canonical_str = json.dumps(canonical_data, sort_keys=True, ensure_ascii=False)
        namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
        return str(uuid.uuid5(namespace, canonical_str))