import os
from sqlmodel import create_engine, SQLModel, Session
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.sql import text

# Import Gold layer models to ensure they are included in SQLModel.metadata
from movies_data_pipeline.data_access.models.gold_tables import RevenueByGenre, AvgScoreByYear

# Get the DATABASE_URL from environment variables (set in docker-compose.yml)
DATABASE_URL = os.getenv("DATABASE_URL")
print(f"DEBUG: DATABASE_URL={DATABASE_URL}")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Extract the database name from the DATABASE_URL
db_name = DATABASE_URL.split("/")[-1]

# Create a connection to the default 'postgres' database to check/create the target database
default_db_url = DATABASE_URL.replace(f"/{db_name}", "/postgres")
default_engine = sqlalchemy_create_engine(default_db_url)

# Check if the database exists, and create it if it doesn't
with default_engine.connect() as conn:
    conn.execute(text("COMMIT"))  # Ensure we're not in a transaction
    result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"))
    if not result.fetchone():
        conn.execute(text(f"CREATE DATABASE {db_name}"))
        print(f"Created database: {db_name}")
    else:
        print(f"Database {db_name} already exists")

# Now create the engine for the target database
engine = create_engine(DATABASE_URL, echo=True)

def init_db():
    """Initialize the database by creating all tables defined in SQLModel metadata."""
    SQLModel.metadata.create_all(engine)

def get_session():
    """
    Generator function for FastAPI dependency injection.
    Yields a database session for use in FastAPI routes.
    """
    with Session(engine) as session:
        yield session

def get_session_direct():
    """
    Returns a Session object directly for use in non-FastAPI contexts.
    """
    return Session(engine)