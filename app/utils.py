
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
from app.models import Job, JobEntryLog, JobEntryLogRequest, JobEntryStatus, JobStatus, JobUpdate, Manifest

import httpx

SERVICE_URL = settings.SPHEREX_UPLOAD_SERVICE_URL



def  create_manifest(load_id: str, manifest_file: str) -> Manifest:
    log.info(f"Creating manifest at {SERVICE_URL}")
    url = f"{SERVICE_URL}/manifests"
    payload = {
        "load_id": load_id,
        "manifest_file": manifest_file,
    }
    with httpx.Client(timeout=60) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
    data = r.json()
    return Manifest(**data)


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

def create_job(manifest_id: UUID, mock: bool = False, count: Optional[int] = None) -> Job:
    log.info(f"Creating job for manifest_id {manifest_id}")
    url = f"{SERVICE_URL}/jobs"
    payload = {
        "manifest_id": str(manifest_id),
        "mock": mock,
    }
    if count is not None:
        payload["count"] = count
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
        uploaded_files: int | None = None,
) -> Job:
    # log.info(f"Updating job {job_id}")

    job_update = JobUpdate()
    job_update.status = status
    job_update.completed_at = completed_at
    job_update.updated_at = get_current_time()
    job_update.started_at = started_at
    job_update.uploaded_files = uploaded_files

    # TODO - increment uploaded files, elapsed time, message

    url = f"{SERVICE_URL}/jobs/{job_id}"
    payload = job_update.model_dump(mode="json")

    with httpx.Client(timeout=60) as client:
        r = client.put(url, json=payload)
        r.raise_for_status()
    data = r.json()
    return Job(**data)


async def post_entry_log(entry_log: JobEntryLogRequest) -> JobEntryLog:
    # log.info(f"Posting job entry log for job_id {entry_log.job_id}")
    url = f"{SERVICE_URL}/jobs/{entry_log.job_id}/entry-logs"
    payload = entry_log.model_dump(mode="json")
    with httpx.Client(timeout=60) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
    data = r.json()
    return JobEntryLog(**data)


def get_current_time() -> datetime:
    """Get the current time in ISO format."""
    return datetime.now().astimezone()


def get_elapsed_time(start_time: datetime, end_time: Optional[datetime] = None) -> str:
    """Get the elapsed time since the start time."""
    if end_time is None:
        end_time = get_current_time()
    elapsed = end_time - start_time
    return str(elapsed)



class JobUploadHandler:
    """Class to handle job upload updates."""

    def __init__(self, job_id: UUID, total_files: int):
        self.job_id = job_id
        self.total_files = total_files
        self.started_at = get_current_time()
        self.uploaded_files = 0
    
    async def handle_job_update(self, status: JobStatus, message: str, 
                                completed_at: Optional[datetime] = None):
        await update_job(
            self.job_id,
            status=status,
            message=message,
            completed_at=completed_at,
            uploaded_files=self.uploaded_files
        )
        
        log.info(message)

    async def handle_job_entry_update(self, entry_log: JobEntryLogRequest):
        if entry_log.status == JobEntryStatus.COMPLETED:
            self.uploaded_files += 1

        elapsed_time = get_elapsed_time(self.started_at) # type: ignore
        message = entry_log.message

        if entry_log.message is not None:
            message = entry_log.message
            message = f"{message} elapsed {elapsed_time} [{self.uploaded_files}/{self.total_files}]"
        
        await post_entry_log(entry_log)
        log.info(message)

def get_job_upload_handler(job_id: UUID, total_files: int) -> JobUploadHandler:
    return JobUploadHandler(job_id, total_files)