import os
from sqlmodel import create_engine, SQLModel, Session
import logging

# Import Gold layer models
from movies_data_pipeline.data_access.models.gold_tables import revenue_by_genre, avg_score_by_year

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Configure logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)  # Enable for debugging

# Create engine with pool pre-ping to check connection health
engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,  
    pool_recycle=3600,  
    pool_size=5,         
    max_overflow=10      
)

def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

def get_session_direct():
    return Session(engine)