# movies_data_pipeline/bronze_service.py
import logging
from pathlib import Path
import pandas as pd
from typing import Tuple, List
from movies_data_pipeline.services.etl_service import ETLService
from movies_data_pipeline.domain.models.bronze import BronzeMovieUpdate
import math
from fastapi import UploadFile

logger = logging.getLogger(__name__)

class BronzeService:
    def __init__(self, bronze_file_path: str = None):
        """Initialize the BronzeService with ETLService and optional bronze file path."""
        self.etl_service = ETLService()
        self.bronze_file_path = Path(bronze_file_path) if bronze_file_path else self.etl_service.bronze_file_path

    def seed_bronze(self, file: UploadFile, temp_file_path: str) -> None:
        """Seed bronze data with an uploaded file and run ETL."""
        with open(temp_file_path, "wb") as f:
            f.write(file.file.read())
        self.process_bronze_data(temp_file_path)

    def process_bronze_data(self, file_path: str):
        """Append data to bronze and run ETL transformation and loading."""
        filename = Path(file_path).name
        try:
            records_appended = self.etl_service.extractor.append_to_bronze(file_path)
            if records_appended == 0:
                logger.info(f"No new records appended from {filename}; skipping ETL")
                return
            logger.info(f"Starting transformation for {filename}")
            gold_tables = self.etl_service.transform()
            if gold_tables:
                self.etl_service.load(gold_tables)
                logger.info(f"ETL completed for {filename}: transformed and loaded {len(gold_tables)} tables")
            else:
                logger.info(f"No new data to transform from {filename}; ETL stopped after silver")
        except Exception as e:
            logger.error(f"ETL processing failed for {filename}: {str(e)}")
            raise

    def get_bronze_paginated(self, page: int, page_size: int) -> Tuple[pd.DataFrame, int, int, int, int]:
        """Fetch a paginated subset of bronze data with pagination metadata."""
        if not self.bronze_file_path.exists():
            logger.info("Bronze file does not exist; returning empty result")
            return pd.DataFrame(), page, page_size, 0, 0
        df = pd.read_parquet(self.bronze_file_path)
        total_records = len(df)
        total_pages = math.ceil(total_records / page_size)
        if page > total_pages:
            page = total_pages if total_pages > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_df = df.iloc[start_idx:end_idx]
        logger.info(f"Fetched {len(paginated_df)} records from bronze (page {page}, size {page_size})")
        return paginated_df, page, page_size, total_records, total_pages

    def update_bronze(self, updates: List[BronzeMovieUpdate]) -> None:
        """Update bronze records by bronze_id, modifying only provided keys."""
        if not self.bronze_file_path.exists():
            raise ValueError("Bronze data not found")
        
        df = pd.read_parquet(self.bronze_file_path)
        updated_ids = {update.bronze_id for update in updates}  # Assuming BronzeMovieUpdate now uses bronze_id
        missing_ids = updated_ids - set(df["bronze_id"])
        if missing_ids:
            raise ValueError(f"bronze_ids not found in bronze: {missing_ids}")
        
        # Convert updates to a dictionary for merging
        update_dicts = {update.bronze_id: update.dict(exclude_unset=True) for update in updates}
        
        # Update only the specified fields for matching bronze_ids
        for bronze_id, update_data in update_dicts.items():
            mask = df["bronze_id"] == bronze_id
            for key, value in update_data.items():
                if key != "bronze_id":  # Skip bronze_id field itself during update
                    df.loc[mask, key] = value
        
        df.to_parquet(self.bronze_file_path)
        gold_tables = self.etl_service.transform()
        if gold_tables:
            self.etl_service.load(gold_tables)
        logger.info(f"Updated {len(updates)} bronze records and triggered ETL")

    def delete_bronze(self, bronze_id: int) -> None:
        """Delete a bronze record by bronze_id and reprocess ETL."""
        if not self.bronze_file_path.exists():
            raise ValueError("Bronze data not found")
        
        df = pd.read_parquet(self.bronze_file_path)
        if bronze_id not in df["bronze_id"].values:
            raise ValueError(f"Record with bronze_id {bronze_id} not found")
        
        df = df[df["bronze_id"] != bronze_id]
        df.to_parquet(self.bronze_file_path)
        
        gold_tables = self.etl_service.transform()
        if gold_tables:
            self.etl_service.load(gold_tables)
        logger.info(f"Deleted bronze record with bronze_id {bronze_id} and triggered ETL")