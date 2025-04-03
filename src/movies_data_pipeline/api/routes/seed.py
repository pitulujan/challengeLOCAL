from fastapi import APIRouter
from movies_data_pipeline.controllers.seed_controller import SeedController

router = APIRouter(prefix="/seed", tags=["seed"])
seed_controller = SeedController()
router.include_router(seed_controller.router)