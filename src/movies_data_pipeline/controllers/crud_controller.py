from fastapi import APIRouter, HTTPException
import pandas as pd
import os
from typing import Dict, Any, List

class CrudController:
    def __init__(self):
        self.router = APIRouter()
        self.bronze_path = "src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet"
        self._register_routes()

    def _register_routes(self):
        @self.router.post("/")
        def create_raw(data: Dict[str, Any]) -> Dict[str, str]:
            """Create a new record in the Bronze layer."""
            df = pd.DataFrame([data])
            if os.path.exists(self.bronze_path):
                existing = pd.read_parquet(self.bronze_path)
                df = pd.concat([existing, df], ignore_index=True)
            df.to_parquet(self.bronze_path, index=False)
            return {"message": "Raw data created"}

        @self.router.get("/{movie_name}")
        def read_raw(movie_name: str) -> List[Dict[str, Any]]:
            """Read a record from the Bronze layer by movie_name."""
            df = pd.read_parquet(self.bronze_path)
            result = df[df["names"] == movie_name]
            if result.empty:
                raise HTTPException(status_code=404, detail="Movie not found")
            return result.to_dict(orient="records")

        @self.router.put("/{movie_name}")
        def update_raw(movie_name: str, data: Dict[str, Any]) -> Dict[str, str]:
            """Update a record in the Bronze layer by movie_name."""
            df = pd.read_parquet(self.bronze_path)
            if 'name' not in df.columns:
                raise KeyError(f"'name' column not found in raw data. Available columns: {df.columns.tolist()}")
            if not (df["name"] == movie_name).any():
                raise HTTPException(status_code=404, detail="Movie not found")
            for key, value in data.items():
                df.loc[df["name"] == movie_name, key] = value
            df.to_parquet(self.bronze_path, index=False)
            return {"message": "Raw data updated"}

        @self.router.delete("/{movie_name}")
        def delete_raw(movie_name: str) -> Dict[str, str]:
            """Delete a record from the Bronze layer by movie_name."""
            df = pd.read_parquet(self.bronze_path)
            if 'name' not in df.columns:
                raise KeyError(f"'name' column not found in raw data. Available columns: {df.columns.tolist()}")
            if not (df["name"] == movie_name).any():
                raise HTTPException(status_code=404, detail="Movie not found")
            df = df[df["name"] != movie_name]
            df.to_parquet(self.bronze_path, index=False)
            return {"message": "Raw data deleted"}