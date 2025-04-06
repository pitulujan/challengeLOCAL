from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from pathlib import Path
import shutil
import logging
from movies_data_pipeline.services.bronze_service import BronzeService
import os

logger = logging.getLogger(__name__)

class SeedController:
    def __init__(self):
        """Initialize the SeedController with router and BronzeService."""
        self.router = APIRouter()
        self.bronze_service = BronzeService()
        self.bronze_base_path = Path(os.getenv("BRONZE_BASE_PATH"))
        if not self.bronze_base_path:
            raise ValueError("BRONZE_BASE_PATH environment variable not set")
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        async def seed_data(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
            """Seed data by saving the uploaded file to bronze and processing it in the background.
            
            Args:
                file: Uploaded file (CSV, JSON, or PDF)
                background_tasks: FastAPI BackgroundTasks for processing
            
            Returns:
                Dict with success message
            
            Raises:
                HTTPException: If file type is unsupported or saving fails
            """
            # Validate file type
            file_type = file.filename.split(".")[-1].lower()
            if file_type not in ["csv", "json", "pdf"]:
                raise HTTPException(status_code=400, detail="Unsupported file type. Use 'csv', 'json', or 'pdf'")

            raw_file_path = self.bronze_base_path / file.filename

            try:
                # Save the uploaded file as-is to bronze directory
                with raw_file_path.open("wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                logger.info(f"Raw file saved to bronze layer at {raw_file_path}")

                # Schedule processing in the background using the file path
                logger.info(f"Scheduling bronze processing for {file.filename}")
                background_tasks.add_task(self.bronze_service.process_bronze_data, str(raw_file_path))
                return {"message": f"Data seeding started for {file.filename}, processing in the background"}
            except Exception as e:
                logger.error(f"Failed to save file {file.filename} to bronze: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Failed to initiate seeding: {str(e)}")