import logging
from pathlib import Path
from movies_data_pipeline.services.etl_service import ETLService

logger = logging.getLogger(__name__)

class BronzeService:
    def __init__(self):
        """Initialize the BronzeService with ETLService."""
        self.etl_service = ETLService()

    def process_bronze_data(self, file_path: str):
        """Append data to bronze and run ETL transformation and loading.
        
        Args:
            file_path: Path to the raw file in bronze layer
        """
        filename = Path(file_path).name
        try:
            # Append to bronze/movies.parquet
            records_appended = self.etl_service.extractor.append_to_bronze(file_path)
            if records_appended == 0:
                logger.info(f"No new records appended from {filename}; skipping ETL")
                return

            # Run transformation (includes deduplication in silver)
            logger.info(f"Starting transformation for {filename}")
            gold_tables = self.etl_service.transform()
            
            if gold_tables:  # Only load if there’s new data
                self.etl_service.load(gold_tables)
                logger.info(f"ETL completed for {filename}: transformed and loaded {len(gold_tables)} tables")
            else:
                logger.info(f"No new data to transform from {filename}; ETL stopped after silver")
        
        except Exception as e:
            logger.error(f"ETL processing failed for {filename}: {str(e)}")
            raise