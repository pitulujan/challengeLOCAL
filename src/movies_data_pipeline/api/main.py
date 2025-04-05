from fastapi import FastAPI,Request
from movies_data_pipeline.api.routes import seed, crud, gold, search
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.data_access.database import init_db
from movies_data_pipeline.services.initialize_service import initialize_schemas
import logging

# Configure logging to show only INFO and above
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()  # Root logger
logger.setLevel(logging.INFO)

# Explicitly set SQLAlchemy logger to INFO
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)  # Suppress DEBUG from SQLAlchemy
logging.getLogger("python_multipart").setLevel(logging.INFO)

app = FastAPI()

app.include_router(seed.router, prefix="/seed", tags=["Seed"])
app.include_router(crud.router, prefix="/raw", tags=["CRUD"])
app.include_router(gold.router, prefix="/gold", tags=["Gold"])
app.include_router(search.router, prefix="/search", tags=["Search"])



app = FastAPI()

# Include all routers
app.include_router(seed.router)
app.include_router(crud.router)
app.include_router(gold.router)
app.include_router(search.router)

@app.on_event("startup")
def startup_event():
    init_db()
    initialize_schemas()
    vector_db = VectorDB()
    vector_db._initialize_collection()
    
