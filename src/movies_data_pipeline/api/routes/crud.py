from fastapi import APIRouter
from movies_data_pipeline.controllers.crud_controller import CrudController

router = APIRouter(prefix="/raw", tags=["crud"])
crud_controller = CrudController()
router.include_router(crud_controller.router)