from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class Status(BaseModel):
    """Model representing the status of the application."""

    status: str
    service_name: Optional[str] = None
    service_version: Optional[str] = None
    hostname: Optional[str] = None
    startup_time: Optional[datetime] = None
    remote_time: Optional[datetime] = None


class HealthCheck(BaseModel):
    """Model representing the health check status of the application."""

    status: str
    service_name: Optional[str] = None
    hostname: Optional[str] = None
    startup_time: Optional[datetime] = None
    remote_time: Optional[datetime] = None