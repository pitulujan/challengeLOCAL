from fastapi import APIRouter
import pandas as pd
from typing import List, Dict, Any
from movies_data_pipeline.data_access.database import get_session_direct  # Import get_session_direct

class GoldController:
    def __init__(self):
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self):
        @self.router.get("/revenue_by_genre")
        def get_revenue_by_genre() -> List[Dict[str, Any]]:
            """Query total revenue by genre from the Gold layer in PostgreSQL."""
            with get_session_direct() as session:  # Use get_session_direct instead of engine
                result = pd.read_sql("SELECT * FROM revenue_by_genre", session.get_bind())
            return result.to_dict(orient="records")

        @self.router.get("/avg_score_by_year")
        def get_avg_score_by_year() -> List[Dict[str, Any]]:
            """Query average score by year from the Gold layer in PostgreSQL."""
            with get_session_direct() as session:  # Use get_session_direct instead of engine
                result = pd.read_sql("SELECT * FROM avg_score_by_year", session.get_bind())
            return result.to_dict(orient="records")