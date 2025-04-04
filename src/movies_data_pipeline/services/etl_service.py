import pandas as pd
from typing import Dict, Any
from fastapi import UploadFile, BackgroundTasks
import logging
import threading
from movies_data_pipeline.services.search_service import SearchService

from .extractor_service import Extractor
from .transformer_service import Transformer
from .loader_service import Loader
from .search_service_adapter import SearchServiceAdapter

logger = logging.getLogger(__name__)

class ETLService:
    def __init__(self):
        """Initialize the ETL Service with paths and component classes."""
        self.bronze_path = "src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet"
        self.silver_base_path = "src/movies_data_pipeline/data_access/data_lake/silver/"
        self.gold_base_path = "src/movies_data_pipeline/data_access/data_lake/gold/"
        
        self.extractor = Extractor(self.bronze_path)
        self.transformer = Transformer(self.bronze_path)
        self.loader = Loader(self.silver_base_path, self.gold_base_path)
        self.search_adapter = SearchServiceAdapter(self.bronze_path)
        self._exceptions = []  # Store exceptions from threads

    def extract(self, file: UploadFile, background_tasks: BackgroundTasks = None, batch_size: int = 1000, use_threads: bool = True) -> pd.DataFrame:
        """Extract data from file and process ETL and indexing in parallel if use_threads is True."""
        try:
            # Extract and cleanse data
            df = self.extractor.extract(file, batch_size=batch_size)
            
            if use_threads:
                # Event to signal extraction completion
                extract_complete = threading.Event()
                
                # Threads for ETL and indexing
                etl_thread = threading.Thread(target=self._run_full_etl_thread, args=(extract_complete,))
                index_thread = threading.Thread(target=self._run_indexing_thread, args=(df, batch_size, extract_complete))
                
                # Start threads
                etl_thread.start()
                index_thread.start()
                
                # Signal extraction complete
                extract_complete.set()
                
                # Wait for threads to finish and check for exceptions
                etl_thread.join()
                index_thread.join()
                
                if self._exceptions:
                    raise Exception("Errors occurred in threads: " + "; ".join(str(e) for e in self._exceptions))
            else:
                # Sequential execution
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i + batch_size].to_dict('records')
                    self.search_adapter.batch_create_documents(batch)
                if background_tasks:
                    background_tasks.add_task(self._run_full_etl)
                else:
                    self._run_full_etl()
            
            return df
        except Exception as e:
            logger.error(f"ETL extract phase failed: {str(e)}")
            raise
    
    def _run_full_etl_thread(self, extract_complete: threading.Event):
        """Run the full ETL process in a thread."""
        try:
            extract_complete.wait()  # Wait for extraction to complete
            logger.info("Starting full ETL process in thread")
            transformed_data = self.transform()
            self.load(transformed_data)
            logger.info("Full ETL process completed in thread")
        except Exception as e:
            self._exceptions.append(e)
            logger.error(f"Full ETL process failed in thread: {str(e)}")

    def _run_indexing_thread(self, df: pd.DataFrame, batch_size: int, extract_complete: threading.Event):
        """Run Typesense indexing in a thread."""
        try:
            extract_complete.wait()  # Wait for extraction to complete
            logger.info("Starting Typesense indexing in thread")
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size].to_dict('records')
                self.search_adapter.batch_create_documents(batch)
            logger.info("Typesense indexing completed in thread")
        except Exception as e:
            self._exceptions.append(e)
            logger.error(f"Typesense indexing failed in thread: {str(e)}")

    def _run_full_etl(self):
        """Run the full ETL process synchronously."""
        logger.info("Starting full ETL process")
        try:
            transformed_data = self.transform()
            self.load(transformed_data)
            logger.info("Full ETL process completed")
        except Exception as e:
            logger.error(f"Full ETL process failed: {str(e)}")
            raise

    def transform(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Transform raw data into silver and gold layer tables."""
        return self.transformer.transform()
    
    def load(self, transformed_data: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        """Load transformed data into silver and gold layers."""
        self.loader.load(transformed_data)
    
    def update_typesense(self, operation: str, movie_data: Dict[str, Any], movie_name: str = None) -> None:
        """Update Typesense index with movie data."""
        if operation == "create":
            self.search_adapter.create_document(movie_data)
        elif operation == "update":
            self.search_adapter.update_document(movie_data, movie_name)
        elif operation == "delete":
            self.search_adapter.delete_document(movie_name)
        else:
            raise ValueError(f"Unknown operation: {operation}. Must be 'create', 'update', or 'delete'.")
    
    def run_etl_pipeline(self, file: UploadFile = None, batch_size: int = 1000, use_threads: bool = True) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Run the complete ETL pipeline with optional threading."""
        try:
            if file:
                self.extract(file, batch_size=batch_size, use_threads=use_threads)
            else:
                if use_threads:
                    extract_complete = threading.Event()
                    etl_thread = threading.Thread(target=self._run_full_etl_thread, args=(extract_complete,))
                    index_thread = threading.Thread(target=self._run_indexing_thread, args=(self.extractor.load_bronze_data(), batch_size, extract_complete))
                    etl_thread.start()
                    index_thread.start()
                    extract_complete.set()
                    etl_thread.join()
                    index_thread.join()
                    if self._exceptions:
                        raise Exception("Errors occurred in threads: " + "; ".join(str(e) for e in self._exceptions))
                else:
                    self._run_full_etl()
                    self.sync_search_index(batch_size=batch_size)
            
            transformed_data = self.transform()  # Return the latest transformed data
            logger.info("ETL pipeline completed successfully")
            return transformed_data
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise
    
    def sync_search_index(self, batch_size: int = 1000) -> None:
        """Synchronize the search index with the current bronze data in batches."""
        try:
            self.search_adapter.search_service.clear_index()
            for chunk in pd.read_parquet(self.bronze_path, chunksize=batch_size):
                df = pd.DataFrame(chunk)
                if 'names' in df.columns and 'name' not in df.columns:
                    df = df.rename(columns={'names': 'name'})
                elif 'names' in df.columns:
                    df = df.drop(columns=['names'])
                self.search_adapter.batch_create_documents(df.to_dict('records'))
            logger.info(f"Search index synchronized with bronze data")
        except Exception as e:
            logger.error(f"Search index synchronization failed: {str(e)}")
            raise