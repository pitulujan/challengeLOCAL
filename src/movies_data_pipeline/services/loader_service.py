from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import logging
from sqlalchemy.orm import Session
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.data_access.database import get_session_direct
from typing import Dict
import pandas as pd

logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, silver_file_path: str, db_engine):
        """Initialize the Loader with silver file path and database engine."""
        self.silver_file_path = silver_file_path
        self.db_engine = db_engine
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.db_engine)

    def load_gold(self, gold_tables: dict):
        """Load gold tables into PostgreSQL database using upsert on unique constraints."""
        # Define the correct order for loading to respect foreign key constraints
        table_order = [
            "dim_movie", "dim_date", "dim_country", "dim_language", "dim_crew", "dim_genre",
            "bridge_movie_genre", "bridge_movie_crew", "fact_movie_metrics","revenue_by_genre","avg_score_by_year", "lineage_log"
        ]

        # Define unique constraint columns for each table (based on gold.py models)
        unique_constraints = {
            "dim_movie": ["name", "orig_title"],
            "dim_date": ["year", "month", "day"],
            "dim_country": ["country_name"],
            "dim_language": ["language_name"],
            "dim_crew": ["actor_name", "character_name"],
            "dim_genre": ["genre_name"],
            "bridge_movie_genre": ["movie_id", "genre_id"],
            "bridge_movie_crew": ["movie_id", "crew_id", "character_name"],
            "fact_movie_metrics": ["movie_id", "date_id", "country_id", "language_id"],
            "revenue_by_genre": ["genre_name"],
            "avg_score_by_year":["year"],
            "lineage_log": ["lineage_log_id"]  # Use primary key since no business constraint
        }

        try:

            with self.db_engine.connect() as conn:
                with conn.begin():  # Start a transaction
                    for table_name in table_order:
                        if table_name in gold_tables:
                            df = gold_tables[table_name]
                            if df.empty:
                                logger.info(f"No data to load for {table_name}; skipping")
                                continue

                            try:
                                # Get the table object from the database
                                table = Table(table_name, self.metadata, autoload_with=self.db_engine)
                                constraint_cols = unique_constraints[table_name]

                                # Get the list of columns in the database table
                                db_columns = [c.name for c in table.columns]

                                # Filter DataFrame to include only columns that exist in the database
                                df = df[[col for col in df.columns if col in db_columns]]

                                # Convert DataFrame to list of dictionaries for SQLAlchemy
                                records = df.to_dict("records")

                                # Prepare the upsert statement
                                stmt = insert(table).values(records)
                                if table_name == "lineage_log":
                                    # For lineage_log, append-only (no updates)
                                    stmt = stmt.on_conflict_do_nothing(
                                        index_elements=constraint_cols
                                    )
                                else:
                                    # For other tables, update on conflict
                                    table_columns = [c.name for c in table.columns]
                                    set_ = {col: stmt.excluded[col] for col in table_columns if col in df.columns and col not in constraint_cols}
                                    stmt = stmt.on_conflict_do_update(
                                        index_elements=constraint_cols,
                                        set_=set_
                                    )

                                # Execute the upsert
                                result = conn.execute(stmt)
                                logger.info(f"Upserted {table_name} into PostgreSQL with {len(df)} records (rows affected: {result.rowcount})")
                            except Exception as e:
                                logger.error(f"Failed to upsert {table_name}: {str(e)}")
                                raise

                # Create a session from the engine and sync Typesense
                with Session(self.db_engine) as session:
                    vector_db = VectorDB(initialize=False, db_session=session)
                    vector_db._sync_with_gold()
                    logger.info("Synced Typesense with gold layer data")

        except Exception as e:
            logger.error(f"Error during load_gold: {str(e)}")
            raise