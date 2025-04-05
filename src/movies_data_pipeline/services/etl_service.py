import pandas as pd
from typing import Dict, Any, List
from fastapi import UploadFile, HTTPException
import logging
from .extractor_service import Extractor
from .transformer_service import Transformer
from .loader_service import Loader
from .search_service_adapter import SearchServiceAdapter
from movies_data_pipeline.data_access.vector_db import VectorDB
import json

logger = logging.getLogger(__name__)

class ETLService:
    def __init__(self):
        self.bronze_path = "src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet"
        self.silver_base_path = "src/movies_data_pipeline/data_access/data_lake/silver/"
        self.gold_base_path = "src/movies_data_pipeline/data_access/data_lake/gold/"
        
        self.extractor = Extractor(self.bronze_path)
        self.transformer = Transformer(self.bronze_path)
        self.loader = Loader(self.silver_base_path, self.gold_base_path)
        self.search_adapter = SearchServiceAdapter(self.bronze_path)
        self.vector_db = VectorDB(initialize=False)
        self._exceptions = []

    def extract(self, file_path: str, batch_size: int = 10000) -> tuple[pd.DataFrame, int]:
        """Extract data from file path and index to Typesense only if new records are added."""
        try:
            logger.info(f"Starting extract phase for {file_path}")
            df, new_records_count = self.extractor.extract(file_path, batch_size=batch_size)
            if new_records_count > 0 and not df.empty:
                logger.info("Indexing data to Typesense")
                self.search_adapter.batch_create_documents(df.to_dict('records'), batch_size=batch_size)
                logger.info(f"Extract phase completed for {file_path}, {new_records_count} new records")
            else:
                logger.debug("No new or updated records to index, skipping Typesense")
            return df, new_records_count
        except Exception as e:
            logger.error(f"ETL extract phase failed: {str(e)}")
            raise

    def _run_full_etl(self):
        """Run the full ETL process (transform and load)."""
        try:
            logger.info("Starting full ETL process")
            transformed_data = self.transform()
            self.load(transformed_data)
            logger.info("Full ETL process completed")
        except Exception as e:
            logger.error(f"Full ETL process failed: {str(e)}")
            raise

    def transform(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Transform raw data into silver and gold layer tables."""
        logger.info("Starting transform phase")
        transformed_data = self.transformer.transform()
        logger.info("Transform phase completed")
        return transformed_data
    
    def load(self, transformed_data: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        """Load transformed data into silver and gold layers."""
        logger.info("Starting load phase")
        self.loader.load(transformed_data)
        logger.info("Load phase completed")
    
    def update_typesense(self, operation: str, movie_data: Dict[str, Any], movie_name: str = None) -> None:
        """Update Typesense index with movie data."""
        logger.info(f"Updating Typesense with operation: {operation}")
        if operation == "create":
            self.search_adapter.create_document(movie_data)
        elif operation == "update":
            self.search_adapter.update_document(movie_data, movie_name)
        elif operation == "delete":
            self.search_adapter.delete_document(movie_name)
        else:
            raise ValueError(f"Unknown operation: {operation}. Must be 'create', 'update', or 'delete'.")
    
    def run_etl_pipeline(self, file: UploadFile = None, batch_size: int = 10000) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Run the complete ETL pipeline, only running transform/load for new data."""
        try:
            logger.info("Starting full ETL pipeline")
            if file:
                bronze_dir = '/'.join(self.bronze_path.split('/')[:-1])
                file_path = f"{bronze_dir}/{file.filename}"
                df, new_records_count = self.extract(file_path, batch_size=batch_size)
                if new_records_count > 0:
                    transformed_data = self.transform()
                    self.load(transformed_data)
                    logger.info(f"ETL pipeline completed with {new_records_count} new records")
                    return transformed_data
                else:
                    logger.debug("No new records added, skipping transform and load")
                    return {}
            else:
                self._run_full_etl()
                self.sync_search_index(batch_size=batch_size)
                transformed_data = self.transform()
                logger.info("ETL pipeline completed successfully (no file provided)")
                return transformed_data
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise
    
    def sync_search_index(self, batch_size: int = 10000) -> None:
        """Synchronize the search index with the current bronze data in batches."""
        try:
            logger.info("Starting search index sync")
            self.search_adapter.search_service.clear_index()
            df = self.extractor.load_bronze_data()
            self.search_adapter.batch_create_documents(df.to_dict('records'), batch_size=batch_size)
            logger.info("Search index synchronized with bronze data")
        except Exception as e:
            logger.error(f"Search index synchronization failed: {str(e)}")
            raise

    def batch_update_typesense(self, updates: List[Dict[str, Any]]) -> None:
        """Batch update documents in Typesense."""
        try:
            if not updates:
                logger.info("No updates to process for Typesense")
                return
            
            updates_jsonl = "\n".join(json.dumps(update) for update in updates)
            logger.debug(f"Typesense batch update payload: {updates_jsonl}")
            
            response = self.vector_db.client.collections[self.vector_db.collection_name].documents.import_(
                updates_jsonl,
                {"action": "update", "dirty_values": "coerce_or_drop"}
            )
            
            if isinstance(response, str):
                response_lines = response.strip().split("\n")
                for i, line in enumerate(response_lines):
                    result = json.loads(line)
                    if not result.get("success", False):
                        logger.error(f"Typesense update failed for document {i}: {result.get('error', 'Unknown error')}")
                        raise Exception(f"Typesense update failed: {result.get('error')}")
            elif isinstance(response, list):
                for i, result in enumerate(response):
                    if not result.get("success", False):
                        logger.error(f"Typesense update failed for document {i}: {result.get('error', 'Unknown error')}")
                        raise Exception(f"Typesense update failed: {result.get('error')}")
            
            logger.info(f"Successfully batch updated {len(updates)} documents in Typesense. Response: {response}")
        except Exception as e:
            logger.error(f"Failed to batch update Typesense: {str(e)}")
            raise