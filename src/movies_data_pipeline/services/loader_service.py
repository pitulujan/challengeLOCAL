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
        # Define table categories
        dimension_tables = [
            "dim_movie", "dim_date", "dim_country", "dim_language", "dim_crew", "dim_genre"
        ]
        bridge_fact_tables = [
            "bridge_movie_genre", "bridge_movie_crew", "fact_movie_metrics",
            "revenue_by_genre", "avg_score_by_year"
        ]
        other_tables = ["lineage_log"]

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
            "avg_score_by_year": ["year"],
            "lineage_log": ["lineage_log_id"]
        }

        try:
            id_mappings = {}  # To store lineage_id to generated ID mappings

            with self.db_engine.connect() as conn:
                with conn.begin():  # Start a transaction
                    # Step 1: Upsert dimension tables and retrieve IDs
                    for table_name in dimension_tables:
                        if table_name in gold_tables:
                            df = gold_tables[table_name]
                            if df.empty:
                                continue
                            table = Table(table_name, self.metadata, autoload_with=self.db_engine)
                            constraint_cols = unique_constraints[table_name]
                            db_columns = [c.name for c in table.columns]
                            df = df[[col for col in df.columns if col in db_columns]]
                            # Deduplicate based on constraint columns
                            df = df.drop_duplicates(subset=constraint_cols, keep="last")
                            records = df.to_dict("records")
                            stmt = insert(table).values(records)
                            set_ = {col: stmt.excluded[col] for col in db_columns if col in df.columns and col not in constraint_cols}
                            stmt = stmt.on_conflict_do_update(index_elements=constraint_cols, set_=set_)
                            conn.execute(stmt)
                            logger.info(f"Upserted {table_name} with {len(df)} records")
                            # Retrieve generated IDs
                            id_col = table.primary_key.columns.values()[0].name
                            mapping_df = pd.read_sql(f"SELECT {id_col}, lineage_id FROM {table_name}", conn)
                            id_mappings[table_name] = dict(zip(mapping_df["lineage_id"], mapping_df[id_col]))

                    # Step 2: Populate foreign keys and upsert bridge/fact tables
                    for table_name in bridge_fact_tables:
                        if table_name in gold_tables:
                            df = gold_tables[table_name]
                            if df.empty:
                                continue
                            # Handle specific table transformations
                            if table_name == "bridge_movie_genre":
                                df["movie_id"] = df["movie_lineage_id"].map(id_mappings["dim_movie"])
                                df["genre_id"] = df["genre_lineage_id"].map(id_mappings["dim_genre"])
                                df = df[["movie_id", "genre_id", "lineage_id", "created_at", "updated_at"]]
                            elif table_name == "bridge_movie_crew":
                                df["movie_id"] = df["movie_lineage_id"].map(id_mappings["dim_movie"])
                                df["crew_id"] = df["crew_lineage_id"].map(id_mappings["dim_crew"])
                                df = df[["movie_id", "crew_id", "character_name", "lineage_id", "created_at", "updated_at"]]
                            elif table_name == "fact_movie_metrics":
                                df["movie_id"] = df["movie_lineage_id"].map(id_mappings["dim_movie"])
                                df["date_id"] = df["date_lineage_id"].map(id_mappings["dim_date"])
                                df["country_id"] = df["country_lineage_id"].map(id_mappings["dim_country"])
                                df["language_id"] = df["language_lineage_id"].map(id_mappings["dim_language"])
                                df = df[["movie_id", "date_id", "country_id", "language_id", "budget", "revenue", "score", "lineage_id", "created_at", "updated_at"]]
                            # Deduplicate based on constraint columns
                            table = Table(table_name, self.metadata, autoload_with=self.db_engine)
                            constraint_cols = unique_constraints[table_name]
                            db_columns = [c.name for c in table.columns]
                            df = df[[col for col in df.columns if col in db_columns]]
                            df = df.drop_duplicates(subset=constraint_cols, keep="last")
                            records = df.to_dict("records")
                            stmt = insert(table).values(records)
                            set_ = {col: stmt.excluded[col] for col in db_columns if col in df.columns and col not in constraint_cols}
                            stmt = stmt.on_conflict_do_update(index_elements=constraint_cols, set_=set_)
                            conn.execute(stmt)
                            logger.info(f"Upserted {table_name} with {len(df)} records")

                    # Step 3: Upsert other tables (e.g., lineage_log)
                    for table_name in other_tables:
                        if table_name in gold_tables:
                            df = gold_tables[table_name]
                            if df.empty:
                                continue
                            table = Table(table_name, self.metadata, autoload_with=self.db_engine)
                            constraint_cols = unique_constraints[table_name]
                            db_columns = [c.name for c in table.columns]
                            df = df[[col for col in df.columns if col in db_columns]]
                            # Deduplicate based on constraint columns
                            # Keeping a lightweight deduplication step in the Loader as a fallback, 
                            # using the database’s unique_constraints, to catch any edge cases missed in the Transformer.
                            df = df.drop_duplicates(subset=constraint_cols, keep="last")
                            records = df.to_dict("records")
                            stmt = insert(table).values(records)
                            if table_name == "lineage_log":
                                stmt = stmt.on_conflict_do_nothing(index_elements=constraint_cols)
                            else:
                                set_ = {col: stmt.excluded[col] for col in db_columns if col in df.columns and col not in constraint_cols}
                                stmt = stmt.on_conflict_do_update(index_elements=constraint_cols, set_=set_)
                            conn.execute(stmt)
                            logger.info(f"Upserted {table_name} with {len(df)} records")

                # Sync with Typesense (if applicable)
                with Session(self.db_engine) as session:
                    vector_db = VectorDB(initialize=False, db_session=session)
                    logger.info(f"Initializing sync with gold layer")
                    vector_db._sync_with_gold()
        except Exception as e:
            logger.error(f"Error during load_gold: {str(e)}")
            raise