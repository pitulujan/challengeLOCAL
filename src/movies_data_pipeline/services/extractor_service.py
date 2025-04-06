import pandas as pd
from typing import Tuple
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class Extractor:
    def __init__(self, bronze_file_path: str):
        """Initialize the Extractor with the bronze Parquet file path."""
        self.bronze_file_path = Path(bronze_file_path)

    def extract(self, file_path: str, start_id: int = 0) -> Tuple[pd.DataFrame, int]:
        """Extract data from a file on disk and assign sequential IDs.
        
        Args:
            file_path: Path to the file (CSV, JSON, or PDF)
            start_id: The starting ID for the new data (default 0)
        
        Returns:
            Tuple of (extracted DataFrame with 'id' column, number of records)
        """
        file_type = Path(file_path).suffix[1:].lower()
        filename = Path(file_path).name
        
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "pdf":
            df = pd.DataFrame()
            logger.warning(f"PDF extraction not implemented for {filename}; returning empty DataFrame")
        else:
            raise ValueError(f"Unsupported file type '{file_type}' for {filename}")
        
        # Add sequential 'id' column if not present
        if "bronze_id" not in df.columns:
            num_records = len(df)
            df["bronze_id"] = range(start_id, start_id + num_records)
        
        return df, len(df)

    def append_to_bronze(self, file_path: str) -> int:
        """Append extracted data from a file to bronze/movies.parquet with sequential IDs.
        
        Args:
            file_path: Path to the file (CSV, JSON, or PDF)
        
        Returns:
            Number of records appended
        """
        filename = Path(file_path).name
        try:
            # Determine the starting ID
            if self.bronze_file_path.exists():
                existing_df = pd.read_parquet(self.bronze_file_path, columns=["bronze_id"])
                start_id = existing_df["bronze_id"].max() + 1 if not existing_df.empty else 0
            else:
                start_id = 0
            
            # Extract data with the next sequential IDs
            df, record_count = self.extract(file_path, start_id=start_id)
            if record_count == 0:
                logger.info(f"No data extracted from {filename}; skipping append")
                return 0
            
            # Append to bronze/movies.parquet
            if self.bronze_file_path.exists():
                existing_df = pd.read_parquet(self.bronze_file_path)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
            else:
                combined_df = df
            
            combined_df.to_parquet(self.bronze_file_path)
            logger.info(f"Appended {record_count} records from {filename} to {self.bronze_file_path}")
            return record_count
        
        except Exception as e:
            logger.error(f"Failed to append {filename} to bronze: {str(e)}")
            raise