import pandas as pd
import os
import uuid
from typing import Dict, Any, List
from fastapi import UploadFile
from datetime import datetime
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Extractor:
    def __init__(self, bronze_path: str):
        """Initialize the Extractor with the path to store raw data.
        
        Args:
            bronze_path: Path where extracted data will be stored
        """
        self.bronze_path = bronze_path
    
    def extract(self, file: UploadFile) -> pd.DataFrame:
        """Extract data from uploaded file and save to bronze layer.
        
        Args:
            file: FastAPI UploadFile containing CSV or JSON data
            
        Returns:
            DataFrame containing the extracted and standardized data
            
        Raises:
            ValueError: If file type is not supported
        """
        try:
            file_type = file.filename.split(".")[-1].lower()
            if file_type == "csv":
                df = pd.read_csv(file.file)
            elif file_type == "json":
                df = pd.read_json(file.file)
            else:
                raise ValueError("Unsupported file type. Use 'csv' or 'json'.")
            
            # Standardize column names
            df = self._standardize_columns(df)
            
            # Add metadata
            df = self._add_metadata(df)
            
            # Save to bronze layer
            self._save_to_bronze(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names in the dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with standardized column names
        """
        # Normalize 'names' to 'name' at ingestion
        if 'names' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'names': 'name'})
        elif 'names' in df.columns:
            df = df.drop(columns=['names'])
        
        return df
    
    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns to the dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with metadata columns added
        """
        current_time = datetime.now()
        df['created_at'] = current_time
        df['updated_at'] = current_time
        
        if 'uuid' not in df.columns:
            df['uuid'] = df.apply(self._generate_uuid, axis=1)
        
        return df
    
    def _generate_uuid(self, row: pd.Series) -> str:
        """Generate deterministic UUID for a row based on its content.
        
        Args:
            row: DataFrame row
            
        Returns:
            UUID string
        """
        raw_data = row.drop(['created_at', 'updated_at', 'uuid'], errors='ignore').astype(str).to_dict()
        data_str = ''.join(f"{k}:{v}" for k, v in sorted(raw_data.items()))
        namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
        return str(uuid.uuid5(namespace, data_str))
    
    def _save_to_bronze(self, df: pd.DataFrame) -> None:
        """Save dataframe to bronze layer, handling duplicates.
        
        Args:
            df: DataFrame to save
        """
        if os.path.exists(self.bronze_path):
            existing_df = pd.read_parquet(self.bronze_path)
            existing_df = self._standardize_columns(existing_df)
            
            # Ensure required columns exist
            for col in ['created_at', 'updated_at', 'uuid']:
                if col not in existing_df.columns:
                    if col == 'uuid':
                        existing_df[col] = existing_df.apply(self._generate_uuid, axis=1)
                    else:
                        existing_df[col] = pd.NaT
            
            # Combine with new data and handle duplicates
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['uuid'], keep='last')
        
        df.to_parquet(self.bronze_path, index=False)
        logger.info(f"Saved {len(df)} records to bronze layer")

    def load_bronze_data(self) -> pd.DataFrame:
        """Load and standardize data from the bronze layer.
        
        Returns:
            DataFrame containing the bronze layer data
        """
        if os.path.exists(self.bronze_path):
            df = pd.read_parquet(self.bronze_path)
            df = self._standardize_columns(df)
            
            # Ensure required columns exist
            for col in ['created_at', 'updated_at', 'uuid']:
                if col not in df.columns:
                    if col == 'uuid':
                        df[col] = df.apply(self._generate_uuid, axis=1)
                    else:
                        df[col] = pd.NaT
            return df
        return pd.DataFrame()  # Return empty DataFrame if no data exists

    def extract_from_dicts(self, data_list: List[Dict[str, Any]]) -> pd.DataFrame:
        """Extract data from a list of dictionaries and save to bronze layer.
        
        Args:
            data_list: List of dictionaries containing data
            
        Returns:
            DataFrame containing only the newly added rows
        """
        df_new = pd.DataFrame(data_list)
        df_new = self._standardize_columns(df_new)
        df_new = self._add_metadata(df_new)
        
        existing_df = self.load_bronze_data()
        
        # Combine with existing data and handle duplicates
        df_combined = pd.concat([existing_df, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['uuid'], keep='last')
        
        # Save the combined data
        df_combined.to_parquet(self.bronze_path, index=False)
        
        # Return only the new rows (those not in existing_df)
        new_uuids = set(df_new['uuid']) - set(existing_df['uuid'])
        new_rows = df_combined[df_combined['uuid'].isin(new_uuids)]
        return new_rows