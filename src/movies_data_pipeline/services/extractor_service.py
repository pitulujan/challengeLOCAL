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
        """Initialize the Extractor with the path to store raw data."""
        self.bronze_path = bronze_path
    
    def extract(self, file_path: str, batch_size: int = 1000) -> pd.DataFrame:
        """Extract data from a file path in batches and save to bronze layer.
        
        Args:
            file_path: Path to the file (CSV or JSON)
            batch_size: Number of rows to process in each batch
            
        Returns:
            DataFrame containing the extracted and standardized data (full dataset)
        """
        try:
            file_type = file_path.split(".")[-1].lower()
            full_df = pd.DataFrame()  # To store the entire dataset for return
            
            if file_type == "csv":
                # Process CSV in chunks
                for chunk in pd.read_csv(file_path, chunksize=batch_size):
                    df_chunk = self._process_chunk(chunk)
                    full_df = pd.concat([full_df, df_chunk], ignore_index=True)
                    self._save_to_bronze(df_chunk)
            elif file_type == "json":
                # JSON files are typically smaller; process as a whole
                df = pd.read_json(file_path)
                df = self._process_chunk(df)
                full_df = df
                self._save_to_bronze(df)
            else:
                raise ValueError("Unsupported file type. Use 'csv' or 'json'.")
            
            logger.info(f"Extracted and saved {len(full_df)} records from {file_path}")
            return full_df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _process_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of data: standardize columns, handle data issues, and add metadata.
        
        Args:
            df: Input dataframe chunk
            
        Returns:
            Processed DataFrame chunk
        """
        # Standardize column names
        df = self._standardize_columns(df)

        # Ensure genre and crew columns are string type
        if "genre" in df.columns:
            df["genre"] = df["genre"].astype(str).replace("nan", "")
        if "crew" in df.columns:
            df["crew"] = df["crew"].astype(str).replace("nan", "")
        
        # Add metadata
        df = self._add_metadata(df)
        
        return df
    
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
        """Save dataframe to bronze layer, handling duplicates incrementally.
        
        Args:
            df: DataFrame chunk to save
        """
        if os.path.exists(self.bronze_path):
            existing_df = pd.read_parquet(self.bronze_path)
            existing_df = self._standardize_columns(existing_df)
            
            # Ensure required columns exist in existing data
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
        logger.debug(f"Saved {len(df)} records to bronze layer (including existing)")

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

    def extract_from_dicts(self, data_list: List[Dict[str, Any]], batch_size: int = 1000) -> pd.DataFrame:
        """Extract data from a list of dictionaries in batches and save to bronze layer.
        
        Args:
            data_list: List of dictionaries containing data
            batch_size: Number of records to process in each batch
            
        Returns:
            DataFrame containing only the newly added rows
        """
        full_new_df = pd.DataFrame()  # To store all new rows
        existing_df = self.load_bronze_data()
        
        # Process in batches
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            df_new = pd.DataFrame(batch)
            df_new = self._standardize_columns(df_new)
            df_new = self._add_metadata(df_new)
            
            # Combine with existing data and handle duplicates
            df_combined = pd.concat([existing_df, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['uuid'], keep='last')
            
            # Save the combined data
            df_combined.to_parquet(self.bronze_path, index=False)
            
            # Identify new rows in this batch
            new_uuids = set(df_new['uuid']) - set(existing_df['uuid'])
            new_rows = df_combined[df_combined['uuid'].isin(new_uuids)]
            full_new_df = pd.concat([full_new_df, new_rows], ignore_index=True)
            
            # Update existing_df for the next batch
            existing_df = df_combined
        
        logger.info(f"Extracted {len(full_new_df)} new records from {len(data_list)} dictionaries")
        return full_new_df