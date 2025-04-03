from fastapi import APIRouter, UploadFile, File, HTTPException
from movies_data_pipeline.services.etl_service import ETLService
import os

class SeedController:
    def __init__(self):
        self.router = APIRouter()
        self.etl_service = ETLService()
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        async def seed_data(file: UploadFile = File(...)):
            """
            Seed data by uploading a file (CSV, JSON, or PDF) and running the ETL pipeline.
            """
            # Determine file type
            file_type = file.filename.split(".")[-1].lower()
            if file_type not in ["csv", "json", "pdf"]:
                raise HTTPException(status_code=400, detail="Unsupported file type. Use CSV, JSON, or PDF.")

            # Save the uploaded file temporarily
            temp_file_path = f"/tmp/{file.filename}"
            with open(temp_file_path, "wb") as temp_file:
                temp_file.write(await file.read())

            try:
                # Run ETL pipeline
                self.etl_service.extract(temp_file_path, file_type)
                transformed_data = self.etl_service.transform()
                self.etl_service.load(transformed_data)
                return {"message": "Data seeded successfully"}
            finally:
                # Clean up temporary file
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)