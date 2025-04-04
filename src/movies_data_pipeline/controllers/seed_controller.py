from fastapi import APIRouter, UploadFile, File, HTTPException
from movies_data_pipeline.services.etl_service import ETLService

class SeedController:
    def __init__(self):
        self.router = APIRouter()
        self.etl_service = ETLService()
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        async def seed_data(file: UploadFile = File(...)):
            """
            Seed data by uploading a file (CSV or JSON) and running the ETL pipeline.

            Args:
                file (UploadFile): The uploaded file.

            Returns:
                dict: Success message.

            Raises:
                HTTPException: If the file type is unsupported or processing fails.
            """
            # Check file type
            file_type = file.filename.split(".")[-1].lower()
            if file_type not in ["csv", "json", "pdf"]:
                raise HTTPException(status_code=400, detail="Unsupported file type. Use 'csv', 'json', or 'pdf'.")

            try:
                # Pass UploadFile directly to extract
                self.etl_service.extract(file)
                transformed_data = self.etl_service.transform()
                self.etl_service.load(transformed_data)
                return {"message": "Data seeded successfully"}
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))