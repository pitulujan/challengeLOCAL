import pandas as pd
from typing import Dict, Any
import logging
from sqlalchemy.exc import SQLAlchemyError
from movies_data_pipeline.data_access.database import get_session_direct

logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, silver_base_path: str, gold_base_path: str):
        self.silver_base_path = silver_base_path
        self.gold_base_path = gold_base_path
    
    def load(self, transformed_data: Dict[str, Dict[str, pd.DataFrame]]) -> None:
        try:
            self._load_silver_data(transformed_data["silver"])
            self._load_gold_data(transformed_data["gold"])
            logger.info("Data loading completed successfully")
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            raise
    
    def _load_silver_data(self, silver_data: Dict[str, pd.DataFrame]) -> None:
        for table_name, df in silver_data.items():
            output_path = f"{self.silver_base_path}{table_name}.parquet"
            df.to_parquet(output_path, index=False)
            logger.debug(f"Saved {table_name} to {output_path}")
    
    def _load_gold_data(self, gold_data: Dict[str, pd.DataFrame]) -> None:
        session = get_session_direct()
        try:
            # Log dataframe details for debugging
            for table_name, df in gold_data.items():
                logger.debug(f"Preparing to load {table_name}: {len(df)} rows, columns: {df.columns.tolist()}")

            # Begin a transaction
            with session.begin():
                for table_name, df in gold_data.items():
                    output_path = f"{self.gold_base_path}{table_name}.parquet"
                    df.to_parquet(output_path, index=False)
                    logger.debug(f"Saved {table_name} to {output_path}")
                    
                    try:
                        df.to_sql(table_name, session.get_bind(), if_exists="replace", index=False)
                        logger.debug(f"Saved {table_name} to SQL database")
                    except SQLAlchemyError as sql_e:
                        logger.error(f"Failed to save {table_name} to SQL: {str(sql_e)}")
                        raise

            logger.info("Gold layer data committed to database")
        
        except SQLAlchemyError as e:
            logger.error(f"Database error during gold layer load: {str(e)}")
            session.rollback()
            # Invalidate the connection to force a new one from the pool
            if session.get_bind().raw_connection().invalidated:
                session.get_bind().raw_connection().invalidate()
            raise
        except Exception as e:
            logger.error(f"Unexpected error during gold layer load: {str(e)}")
            session.rollback()
            raise
        finally:
            if session.is_active:
                session.close()
            logger.debug("Database session closed")