from fastapi import FastAPI
from movies_data_pipeline.api.routes import seed, crud, gold, search
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.data_access.database import init_db, get_session_direct
from movies_data_pipeline.services.initialize_service import InitializeService
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Suppress overly verbose SQLAlchemy logs if not needed
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

app = FastAPI(title="Movies Data Pipeline")


app.include_router(seed.router, prefix="", tags=["seed"])
app.include_router(crud.router, prefix="", tags=["bronze"])
app.include_router(gold.router, prefix="", tags=["gold"])
app.include_router(search.router, prefix="", tags=["search"])

@app.on_event("startup")
async def startup_event():
    """Initialize database, schemas, and VectorDB on startup."""
    logger.info("Starting application initialization")
    init_db()  # Create gold layer tables
    initialize_service = InitializeService()
    initialize_service.initialize_schemas()  
    
    # Initialize VectorDB with a database session for gold layer sync
    with get_session_direct() as session:
        vector_db = VectorDB(initialize=True, db_session=session)
    logger.info("Application initialized successfully")