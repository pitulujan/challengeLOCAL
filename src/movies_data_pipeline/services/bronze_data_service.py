from typing import Dict, Any, List
from fastapi import HTTPException
import pandas as pd
import os

class BronzeDataService:
    def __init__(self, bronze_path: str):
        self.bronze_path = bronze_path

    def create(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Create a new record in the Bronze layer."""
        df = pd.DataFrame([data])
        if os.path.exists(self.bronze_path):
            existing = pd.read_parquet(self.bronze_path)
            df = pd.concat([existing, df], ignore_index=True)
        df.to_parquet(self.bronze_path, index=False)
        return {"message": "Raw data created"}

    def read(self, movie_name: str) -> List[Dict[str, Any]]:
        """Read a record from the Bronze layer by movie_name."""
        df = pd.read_parquet(self.bronze_path)
        result = df[df["names"] == movie_name]
        if result.empty:
            raise HTTPException(status_code=404, detail="Movie not found")
        return result.to_dict(orient="records")

    def update(self, movie_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a record in the Bronze layer by movie_name and return the updated record."""
        df = pd.read_parquet(self.bronze_path)
        if not (df["names"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        
        # Validate that the keys in data match existing columns (optional, but recommended)
        valid_columns = df.columns.tolist()
        invalid_keys = [key for key in data.keys() if key not in valid_columns]
        if invalid_keys:
            raise HTTPException(status_code=400, detail=f"Invalid keys: {invalid_keys}")
        
        # Update the record
        for key, value in data.items():
            df.loc[df["names"] == movie_name, key] = value
        df.to_parquet(self.bronze_path, index=False)
        
        # Return the updated record
        updated_record = df[df["names"] == movie_name].iloc[0].to_dict()
        return updated_record

    def delete(self, movie_name: str) -> Dict[str, str]:
        """Delete a record from the Bronze layer by movie_name."""
        df = pd.read_parquet(self.bronze_path)
        if "names" not in df.columns:
            raise KeyError(f"'names' column not found in raw data. Available columns: {df.columns.tolist()}")
        if not (df["names"] == movie_name).any():
            raise HTTPException(status_code=404, detail="Movie not found")
        df = df[df["names"] != movie_name]
        df.to_parquet(self.bronze_path, index=False)
        return {"message": "Raw data deleted"}