import pandas as pd
from typing import Dict, Any
import logging
from movies_data_pipeline.data_access.database import get_session_direct

logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, silver_base_path: str, gold_base_path: str):
        """Initialize the Loader.
        
        Args:
            silver_base_path: Path to store silver layer data
            gold_base_path: Path to store gold layer data
        """
        self.silver_base_path = silver_base_path
        self.gold_base_path = gold_base_path
    
    def load(self, transformed_data: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        """Load transformed data into silver and gold layers.
        
        Args:
            transformed_data: Dictionary containing silver and gold layer dataframes
        """
        try:
            # Load silver layer data
            self._load_silver_data(transformed_data["silver"])
            
            # Load gold layer data
            self._load_gold_data(transformed_data["gold"])
            
            logger.info("Data loading completed successfully")
            
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            raise
    
    def _load_silver_data(self, silver_data: Dict[str, pd.DataFrame]) -> None:
        """Load silver layer data to parquet files.
        
        Args:
            silver_data: Dictionary of silver layer dataframes
        """
        for table_name, df in silver_data.items():
            output_path = f"{self.silver_base_path}{table_name}.parquet"
            df.to_parquet(output_path, index=False)
            logger.debug(f"Saved {table_name} to {output_path}")
    
    def _load_gold_data(self, gold_data: Dict[str, pd.DataFrame]) -> None:
        """Load gold layer data to parquet files and SQL database.
        
        Args:
            gold_data: Dictionary of gold layer dataframes
        """
        with get_session_direct() as session:
            for table_name, df in gold_data.items():
                # Save to parquet
                output_path = f"{self.gold_base_path}{table_name}.parquet"
                df.to_parquet(output_path, index=False)
                logger.debug(f"Saved {table_name} to {output_path}")
                
                # Save to SQL
                df.to_sql(table_name, session.get_bind(), if_exists="replace", index=False)
                logger.debug(f"Saved {table_name} to SQL database")
            
            session.commit()