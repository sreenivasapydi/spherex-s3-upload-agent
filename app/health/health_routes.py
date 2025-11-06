import socket
from fastapi import APIRouter

from app.config import settings
from app.health.health_schemas import Status, HealthCheck
from app.utils import get_current_time

router = APIRouter()

startup_time = get_current_time()


@router.get("/status", response_model=Status)
def get_status():
    """Get the current status of the application."""

    current_time = get_current_time()
    status = Status(
        service_name=settings.SERVICE_NAME,
        status="UP",
        hostname=socket.getfqdn(),
        startup_time=startup_time,
        remote_time=current_time,
    )
    return status


@router.get("/health", response_model=HealthCheck)
async def get_health_check():
    """Get the health check status of the application."""

    current_time = get_current_time()
    health_check = HealthCheck(
        service_name=settings.SERVICE_NAME,
        status="UP",
        hostname=socket.getfqdn(),
        startup_time=startup_time,
        remote_time=current_time,
    )
    return health_check