import pandas as pd
from typing import Tuple
from pathlib import Path
import logging
import uuid

logger = logging.getLogger(__name__)

class Extractor:
    def __init__(self, bronze_file_path: str):
        """Initialize the Extractor with the bronze Parquet file path."""
        self.bronze_file_path = Path(bronze_file_path)

    def extract(self, file_path: str) -> Tuple[pd.DataFrame, int]:
        """Extract data from a file on disk.
        
        Args:
            file_path: Path to the file (CSV, JSON, or PDF)
        
        Returns:
            Tuple of (extracted DataFrame, number of records)
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
        
        # Add uuid if not present
        if "uuid" not in df.columns:
            df["uuid"] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        return df, len(df)

    def append_to_bronze(self, file_path: str) -> int:
        """Append extracted data from a file to bronze/movies.parquet.
        
        Args:
            file_path: Path to the file (CSV, JSON, or PDF)
        
        Returns:
            Number of records appended
        """
        filename = Path(file_path).name
        try:
            df, record_count = self.extract(file_path)
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