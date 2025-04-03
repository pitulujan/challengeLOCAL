from fastapi import APIRouter
from movies_data_pipeline.controllers.gold_controller import GoldController

router = APIRouter(prefix="/gold", tags=["gold"])
gold_controller = GoldController()
router.include_router(gold_controller.router)