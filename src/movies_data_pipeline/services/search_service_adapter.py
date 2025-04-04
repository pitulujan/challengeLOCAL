from typing import Dict, Any
from movies_data_pipeline.services.search_service import SearchService
import logging
import uuid
import pandas as pd

logger = logging.getLogger(__name__)

class SearchServiceAdapter:
    def __init__(self, bronze_path: str):
        """Initialize the SearchServiceAdapter.
        
        Args:
            bronze_path: Path to the bronze layer data
        """
        self.bronze_path = bronze_path
        self.search_service = SearchService()
    
    def create_document(self, movie_data: Dict[str, Any]) -> None:
        """Create a search document for a movie.
        
        Args:
            movie_data: Movie data dictionary
        """
        try:
            self._update_typesense("create", movie_data)
            logger.info(f"Created search document for movie '{movie_data.get('name', '')}'")
        except Exception as e:
            logger.error(f"Failed to create search document: {str(e)}")
            raise
    
    def update_document(self, movie_data: Dict[str, Any], movie_name: str) -> None:
        """Update a search document for a movie.
        
        Args:
            movie_data: Movie data dictionary
            movie_name: Name of the movie to update
        """
        try:
            self._update_typesense("update", movie_data, movie_name)
            logger.info(f"Updated search document for movie '{movie_name}'")
        except Exception as e:
            logger.error(f"Failed to update search document: {str(e)}")
            raise
    
    def delete_document(self, movie_name: str) -> None:
        """Delete a search document for a movie.
        
        Args:
            movie_name: Name of the movie to delete
        """
        try:
            self._update_typesense("delete", {}, movie_name)
            logger.info(f"Deleted search document for movie '{movie_name}'")
        except Exception as e:
            logger.error(f"Failed to delete search document: {str(e)}")
            raise
    
    def _update_typesense(self, operation: str, movie_data: Dict[str, Any], movie_name: str = None) -> None:
        """Update Typesense index with movie data.
        
        Args:
            operation: Operation type ("create", "update", or "delete")
            movie_data: Movie data dictionary
            movie_name: Name of the movie (required for update/delete)
            
        Raises:
            ValueError: If movie_name is missing for delete operation
        """
        if operation == "delete":
            if not movie_name:
                raise ValueError("movie_name is required for delete operation")
                
            df = pd.read_parquet(self.bronze_path)
            if 'names' in df.columns and 'name' not in df.columns:
                df = df.rename(columns={'names': 'name'})
            elif 'names' in df.columns:
                df = df.drop(columns=['names'])
                
            movie_row = df[df["name"] == movie_name]
            if movie_row.empty:
                logger.warning(f"Movie '{movie_name}' not found in bronze layer for deletion")
                return
                
            uuid_to_delete = movie_row.iloc[0]["uuid"]
            self.search_service.delete_movie(uuid_to_delete)
            logger.info(f"Deleted movie '{movie_name}' with UUID '{uuid_to_delete}' from Typesense")
            return

        raw_df = pd.DataFrame([movie_data])
        if "names" in raw_df.columns and "name" not in raw_df.columns:
            raw_df = raw_df.rename(columns={"names": "name"})
        elif "names" in raw_df.columns:
            raw_df = raw_df.drop(columns=["names"])
        
        if "uuid" not in raw_df.columns and movie_name:
            df = pd.read_parquet(self.bronze_path)
            if 'names' in df.columns and 'name' not in df.columns:
                df = df.rename(columns={'names': 'name'})
            elif 'names' in df.columns:
                df = df.drop(columns=['names'])
                
            movie_row = df[df["name"] == movie_name]
            if not movie_row.empty:
                raw_df["uuid"] = movie_row.iloc[0]["uuid"]
            else:
                logger.warning(f"Movie '{movie_name}' not found; generating new UUID")
                raw_df["uuid"] = str(uuid.uuid4())
        
        # Process date fields
        possible_date_cols = ["date_x", "release_date", "date"]
        date_col = next((col for col in possible_date_cols if col in raw_df.columns), None)
        if date_col:
            raw_df["date_x"] = pd.to_datetime(raw_df[date_col], format="%m/%d/%Y", errors="coerce")
        else:
            raw_df["date_x"] = pd.NaT

        # Process genre and crew
        raw_df["genre_list"] = raw_df["genre"].str.split(",\s+") if "genre" in raw_df.columns else [[]]
        
        def parse_crew(crew_str):
            if pd.isna(crew_str) or crew_str == "":
                return []
            crew_list = crew_str.split(", ")
            pairs = []
            for i in range(0, len(crew_list), 2):
                if i + 1 < len(crew_list):
                    pairs.append({"name": crew_list[i], "character_name": crew_list[i + 1]})
                else:
                    pairs.append({"name": crew_list[i], "character_name": "Self"})
            return pairs
            
        raw_df["crew"] = raw_df["crew"].apply(parse_crew) if "crew" in raw_df.columns else [[]]

        # Create search document
        row = raw_df.iloc[0]
        release_date = row["date_x"].strftime("%Y-%m-%d") if pd.notna(row["date_x"]) else "Unknown"
        movie_dict = {
            "id": row["uuid"],
            "name": row["name"],
            "orig_title": row.get("orig_title", row["name"]),
            "overview": row.get("overview", ""),
            "status": row.get("status", "Unknown"),
            "release_date": release_date,
            "genres": row["genre_list"],
            "crew": row["crew"],
            "country": row.get("country", ""),
            "language": row.get("orig_lang", ""),
            "budget": float(row.get("budget_x", 0)),
            "revenue": float(row.get("revenue", 0)),
            "score": float(row.get("score", 0)),
            "is_deleted": False
        }
        
        self.search_service.index_movie(movie_dict)
        logger.info(f"Updated Typesense with {operation} for movie '{row['name']}' with UUID '{row['uuid']}'")