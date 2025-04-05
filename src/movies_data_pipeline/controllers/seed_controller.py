from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
import os
from pathlib import Path
import shutil
import logging
from movies_data_pipeline.services.etl_service import ETLService

logger = logging.getLogger(__name__)

class SeedController:
    def __init__(self):
        self.router = APIRouter()
        self.etl_service = ETLService()
        self.bronze_dir = Path(os.getenv("BRONZE_BASE_PATH"))
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        async def seed_data(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
            file_type = file.filename.split(".")[-1].lower()
            if file_type not in ["csv", "json", "pdf"]:
                raise HTTPException(status_code=400, detail="Unsupported file type. Use 'csv' or 'json'")
            
            os.makedirs(self.bronze_dir, exist_ok=True)
            file_path = os.path.join(self.bronze_dir, file.filename)
            
            try:
                # Save the uploaded file to disk
                with open(file_path, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                logger.debug(f"File saved to {file_path}")
                
                logger.info(f"Scheduling ETL for {file_path} in background")
                background_tasks.add_task(self.run_etl_in_background, file_path)
                return {"message": "Data seeding started in the background"}
            except Exception as e:
                logger.error(f"Failed to process file {file.filename}: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Failed to initiate seeding: {str(e)}")

    def run_etl_in_background(self, file_path: str):
        """Run the ETL process in the background, respecting new_records_count."""
        try:
            logger.info(f"Starting background ETL for {os.path.basename(file_path)}")
            # Create a mock UploadFile object for run_etl_pipeline
            with open(file_path, "rb") as f:
                file = UploadFile(filename=os.path.basename(file_path), file=f)
                result = self.etl_service.run_etl_pipeline(file=file)
            if not result:  # Empty dict means no new records, transform/load skipped
                logger.info(f"Background ETL skipped for {os.path.basename(file_path)} (no new records)")
            else:
                logger.info(f"Background ETL completed for {os.path.basename(file_path)}")
        except Exception as e:
            logger.error(f"Background ETL failed for {os.path.basename(file_path)}: {str(e)}")
            raise