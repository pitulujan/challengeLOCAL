import pandas as pd
from typing import Dict
import logging
from sqlalchemy import create_engine
from .extractor_service import Extractor
from .transformer_service import Transformer
from .loader_service import Loader
from .search_service_adapter import SearchServiceAdapter
from movies_data_pipeline.data_access.vector_db import VectorDB
import os
from pathlib import Path

logger = logging.getLogger(__name__)

class ETLService:
    def __init__(self):
        """Initialize the ETLService with paths and components."""
        self.bronze_file_path = Path(os.getenv("BRONZE_BASE_PATH")) / "bronze_movies.parquet"
        self.silver_file_path = Path(os.getenv("SILVER_BASE_PATH")) / "silver_movies.parquet"
        self.db_engine = create_engine(os.getenv("DATABASE_URL"))
        
        self.extractor = Extractor(self.bronze_file_path)
        self.transformer = Transformer(self.bronze_file_path, self.silver_file_path)
        self.loader = Loader(self.silver_file_path, self.db_engine)
        self.search_adapter = SearchServiceAdapter(self.bronze_file_path)
        self.vector_db = VectorDB(initialize=False)

    def transform(self, new_file_path: str) -> Dict[str, pd.DataFrame]:
        """Transform new file data to silver and gold layers."""
        logger.info("Starting transform phase")
        transformed_data = self.transformer.transform(new_file_path)
        logger.info("Transform phase completed")
        return transformed_data

    def load(self, gold_tables: Dict[str, pd.DataFrame]) -> None:
        """Load gold tables into PostgreSQL."""
        logger.info("Starting load phase")
        self.loader.load_gold(gold_tables)
        logger.info("Load phase completed")
