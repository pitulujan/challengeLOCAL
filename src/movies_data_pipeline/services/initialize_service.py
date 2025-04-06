import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class InitializeService:
    def __init__(self):
        """Initialize directory paths for bronze and silver layers from environment variables."""

        self.bronze_base_path = Path(os.getenv("BRONZE_BASE_PATH"))
        self.silver_base_path = Path(os.getenv("SILVER_BASE_PATH"))

        if not self.bronze_base_path or not self.silver_base_path:
            raise ValueError("BRONZE_BASE_PATH and SILVER_BASE_PATH must be set in environment variables")

        # Define specific file paths within the directories
        self.bronze_file_path = self.bronze_base_path / "bronze_movies.parquet"
        self.silver_file_path = self.silver_base_path / "silver_movies.parquet"

        # Define schemas for bronze and silver layers
        self.bronze_schema = {
            "bronze_id": str,
            "name": str,
            "orig_title": str,
            "overview": str,
            "status": str,
            "date_x": str,  # Raw date string, processed in silver
            "genre": str,   # Comma-separated string
            "crew": str,    # Comma-separated string
            "country": str,
            "orig_lang": str,
            "budget_x": float,
            "revenue": float,
            "score": float
        }

        self.silver_schema = {
            "silver_id": str,
            "bronze_id": str,
            "name": str,
            "orig_title": str,
            "overview": str,
            "status": str,
            "date_x": "datetime64[ns]",  # Processed datetime
            "genre_list": object,        # List of genres
            "crew_pairs": object,        # List of dicts with actor_name, character_name
            "country": str,
            "orig_lang": str,
            "budget_x": float,
            "revenue": float,
            "score": float,
            "created_at": "datetime64[ns]",
            "updated_at": "datetime64[ns]",
            "lineage_id": str
        }

    def initialize_schemas(self):
        """Initialize data lake schemas by creating directories and empty Parquet files if they don’t exist."""
        try:
            # Create directories
            self.bronze_base_path.mkdir(parents=True, exist_ok=True)
            self.silver_base_path.mkdir(parents=True, exist_ok=True)

            # Bronze layer
            if not self.bronze_file_path.exists():
                bronze_df = pd.DataFrame(columns=self.bronze_schema.keys()).astype(self.bronze_schema)
                bronze_df.to_parquet(self.bronze_file_path)
                logger.info(f"Initialized bronze layer schema at {self.bronze_file_path}")

            # Silver layer
            if not self.silver_file_path.exists():
                silver_df = pd.DataFrame(columns=self.silver_schema.keys())
                for col, dtype in self.silver_schema.items():
                    silver_df[col] = silver_df[col].astype(dtype)
                silver_df.to_parquet(self.silver_file_path)
                logger.info(f"Initialized silver layer schema at {self.silver_file_path}")

        except Exception as e:
            logger.error(f"Failed to initialize schemas: {str(e)}")
            raise