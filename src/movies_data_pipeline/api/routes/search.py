from fastapi import APIRouter
from movies_data_pipeline.controllers.search_controller import SearchController

router = APIRouter(prefix="/search", tags=["search"])
search_controller = SearchController()
router.include_router(search_controller.router)