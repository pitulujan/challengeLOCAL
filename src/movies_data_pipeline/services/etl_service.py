import pandas as pd
from typing import Dict, Any
from fastapi import UploadFile, BackgroundTasks
import logging
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
    
    def extract(self, file: UploadFile, background_tasks: BackgroundTasks = None, batch_size: int = 1000) -> pd.DataFrame:
        """Extract data from file and trigger ETL process with batch processing.
        
        Args:
            file: FastAPI UploadFile containing data
            background_tasks: FastAPI BackgroundTasks for async processing
            batch_size: Number of rows to process in each batch
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            # Extract data in batches
            df = self.extractor.extract(file, batch_size=batch_size)
            
            # Process in batches for search index
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size].to_dict('records')
                self.search_adapter.batch_create_documents(batch)
            
            # Schedule background ETL if requested
            if background_tasks:
                background_tasks.add_task(self._run_full_etl)
            
            return df
            
        except Exception as e:
            logger.error(f"ETL extract phase failed: {str(e)}")
            raise
    
    def _run_full_etl(self):
        """Run the full ETL process asynchronously."""
        logger.info("Starting full ETL process")
        try:
            transformed_data = self.transform()
            self.load(transformed_data)
            logger.info("Full ETL process completed")
        except Exception as e:
            logger.error(f"Full ETL process failed: {str(e)}")
    
    def transform(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Transform raw data into silver and gold layer tables.
        
        Returns:
            Dictionary containing silver and gold layer dataframes
        """
        return self.transformer.transform()
    
    def load(self, transformed_data: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        """Load transformed data into silver and gold layers. 
        
        Args:
            transformed_data: Dictionary containing silver and gold layer dataframes
        """
        self.loader.load(transformed_data)
    
    def update_typesense(self, operation: str, movie_data: Dict[str, Any], movie_name: str = None) -> None:
        """Update Typesense index with movie data.
        
        Args:
            operation: Operation type ("create", "update", or "delete")
            movie_data: Movie data dictionary
            movie_name: Name of the movie (required for update/delete)
        """
        if operation == "create":
            self.search_adapter.create_document(movie_data)
        elif operation == "update":
            self.search_adapter.update_document(movie_data, movie_name)
        elif operation == "delete":
            self.search_adapter.delete_document(movie_name)
        else:
            raise ValueError(f"Unknown operation: {operation}. Must be 'create', 'update', or 'delete'.")
    
    def run_etl_pipeline(self, file: UploadFile = None, batch_size: int = 1000) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Run the complete ETL pipeline sequentially.
        
        Args:
            file: Optional FastAPI UploadFile to extract new data
            batch_size: Number of rows to process in each batch
            
        Returns:
            Dictionary containing all transformed data tables
        """
        try:
            # Extract phase (if file is provided)
            if file:
                self.extract(file, batch_size=batch_size)
            
            # Transform phase
            transformed_data = self.transform()
            
            # Load phase
            self.load(transformed_data)
            
            logger.info("ETL pipeline completed successfully")
            return transformed_data
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise
    
    def sync_search_index(self, batch_size: int = 1000) -> None:
        """Synchronize the search index with the current bronze data in batches."""
        try:
            # Read bronze data in chunks
            for chunk in pd.read_parquet(self.bronze_path, chunksize=batch_size):
                df = pd.DataFrame(chunk)
                
                # Standardize column names
                if 'names' in df.columns and 'name' not in df.columns:
                    df = df.rename(columns={'names': 'name'})
                elif 'names' in df.columns:
                    df = df.drop(columns=['names'])
                
                # Batch update search index
                self.search_adapter.batch_create_documents(df.to_dict('records'))
            
            logger.info(f"Search index synchronized with bronze data")
            
        except Exception as e:
            logger.error(f"Search index synchronization failed: {str(e)}")
            raise