
from datetime import datetime
from functools import cache, lru_cache
from re import A, M
from sys import maxsize
from tkinter import N
from typing import Optional
from uuid import UUID
from loguru import logger as log
from pydantic import AwareDatetime
from app.config import settings
from app.models import Job, JobStatus, JobUpdate, Manifest

import httpx

SERVICE_URL = settings.SPHEREX_UPLOAD_SERVICE_URL

def get_manifest_by_load_id(load_id: str) -> Manifest:
    log.info(f"Querying manifests from {SERVICE_URL}")
    url = f"{SERVICE_URL}/manifests"
    params = {}
    params['load_id'] = load_id
    with httpx.Client(timeout=60) as client:
        r = client.get(url, params=params)
        r.raise_for_status()

    data = r.json()
    if not data:
        raise ValueError(f"No manifest found for load_id: {load_id}")
    return Manifest(**data[0])

def get_manifest_by_id(manifest_id: UUID, minimal: bool = False) -> Manifest:
    log.info(f"Querying manifest {manifest_id} from {SERVICE_URL}")
    url = f"{SERVICE_URL}/manifests/{manifest_id}"
    params = {'minimal': minimal}
    with httpx.Client(timeout=60) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
    data = r.json()
    return Manifest(**data)


def find_manifest(load_id: str | None = None, manifest_id: UUID | None = None) -> Manifest:
    if manifest_id:
        return get_manifest_by_id(manifest_id)
    elif load_id:
        return get_manifest_by_load_id(load_id)
    else:
        raise ValueError("Either load_id or manifest_id must be provided to find a manifest")


def get_job_by_id(job_id: UUID) -> Job:
    log.info(f"Querying job {job_id} from {SERVICE_URL}")
    url = f"{SERVICE_URL}/jobs/{job_id}"
    with httpx.Client(timeout=60) as client:
        r = client.get(url)
        r.raise_for_status()
    data = r.json()
    return Job(**data)

def get_jobs(manifest_id: Optional[UUID], load_id: Optional[str],
             status: Optional[JobStatus] = None) -> list[Job]:
    log.info(f"Querying jobs from {SERVICE_URL}")
    url = f"{SERVICE_URL}/jobs"
    params = {}
    if manifest_id:
        params["manifest_id"] = manifest_id
    if load_id:
        params["load_id"] = load_id
    if status:
        params["status"] = status.value
    with httpx.Client(timeout=60) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
    data = r.json()
    return [Job(**item) for item in data]

def get_pending_jobs(manifest_id: Optional[UUID], load_id: Optional[str]) -> list[Job]:
    log.info(f"Querying pending jobs from {SERVICE_URL}")
    return get_jobs(manifest_id=manifest_id, load_id=load_id, status=JobStatus.PENDING)

def create_job(manifest_id: UUID) -> Job:
    log.info(f"Creating job for manifest_id {manifest_id}")
    url = f"{SERVICE_URL}/jobs"
    payload = {
        "manifest_id": str(manifest_id)
    }
    log.info(f"Job creation payload: {payload}")
    with httpx.Client(timeout=60) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
    data = r.json()
    return Job(**data)

async def update_job(
        job_id: UUID,
        status: JobStatus|None = None,
        message: str | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        incr_uploaded_files: int = 0,
) -> Job:
    # log.info(f"Updating job {job_id}")

    job_update = JobUpdate()
    job_update.status = status
    job_update.completed_at = completed_at
    job_update.updated_at = get_current_time()
    job_update.started_at = started_at

    # TODO - increment uploaded files, elapsed time, message

    url = f"{SERVICE_URL}/jobs/{job_id}"
    payload = job_update.model_dump(mode="json")

    with httpx.Client(timeout=60) as client:
        r = client.put(url, json=payload)
        r.raise_for_status()
    data = r.json()
    return Job(**data)

def get_current_time() -> datetime:
    """Get the current time in ISO format."""
    return datetime.now().astimezone()


def get_elapsed_time(start_time: datetime, end_time: Optional[datetime] = None) -> str:
    """Get the elapsed time since the start time."""
    if end_time is None:
        end_time = get_current_time()
    elapsed = end_time - start_time
    return str(elapsed)

