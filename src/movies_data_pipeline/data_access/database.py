import os
from sqlmodel import create_engine, SQLModel, Session
import logging
from typing import Generator

from movies_data_pipeline.data_access.models.gold import (
    DimMovie, DimDate, DimCountry, DimLanguage, DimCrew, DimGenre,
    BridgeMovieGenre, BridgeMovieCrew, FactMovieMetrics, LineageLog
)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Configure logging
logger = logging.getLogger(__name__)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO) 

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    echo=False,  
    pool_pre_ping=True, 
    pool_recycle=3600,   
    pool_size=5,         
    max_overflow=10      
)

def init_db():
    """Initialize the database by creating all gold layer tables."""
    try:
        logger.info("Initializing database tables")
        SQLModel.metadata.create_all(engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

def get_session() -> Generator[Session, None, None]:
    """Provide a database session for dependency injection."""
    with Session(engine) as session:
        yield session

def get_session_direct() -> Session:
    """Provide a direct database session for non-dependency use."""
    return Session(engine)

def get_db_engine():
    """Provide the database engine for dependency injection."""
    return engine