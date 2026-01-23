
from datetime import datetime
from functools import cache, lru_cache
from re import A, M
from sys import maxsize
from tkinter import N
from typing import Optional
from uuid import UUID

import httpx
from loguru import logger as log
from pydantic import AwareDatetime

from app.config import settings
from app.models import (
    Job,
    JobEntryLog,
    JobEntryLogRequest,
    JobEntryStatus,
    JobStatus,
    JobUpdate,
    Manifest,
)

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

def list_manifests(load_id: Optional[str] = None) -> list[Manifest]:
    log.info(f"Querying manifests from {SERVICE_URL}")
    url = f"{SERVICE_URL}/manifests"
    params = {}
    if load_id:
        params['load_id'] = load_id
    with httpx.Client(timeout=60) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
    data = r.json()

    # return max 4 manifests
    if len(data) > 4:
        data = data[:4]
    return [Manifest(**item) for item in data]

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
    log.debug(f"Querying jobs from {SERVICE_URL}")
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

def get_running_jobs(manifest_id: Optional[UUID], load_id: Optional[str]) -> list[Job]:
    log.info(f"Querying running jobs from {SERVICE_URL}")
    return get_jobs(manifest_id=manifest_id, load_id=load_id, status=JobStatus.RUNNING)

def get_active_jobs(manifest_id: Optional[UUID], load_id: Optional[str]) -> list[Job]:
    log.info(f"Querying active jobs from {SERVICE_URL}")
    jobs = []
    jobs.extend( get_pending_jobs(manifest_id=manifest_id, load_id=load_id) )
    jobs.extend( get_running_jobs(manifest_id=manifest_id, load_id=load_id) )
    return jobs

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


def print_job_report(job_id: UUID, job: Job | None = None, manifest: Manifest | None = None):
    if job is None:
        job = get_job_by_id(job_id)
    if manifest is None:
        manifest = get_manifest_by_id(job.manifest_id, minimal=True)
    load_id = manifest.load_id

    log.info("")
    log.info(f"Load-ID.    : {load_id}")
    log.info(f"Data Folders: {' '.join(manifest.data_folders)}")
    log.info(f"Total files : {manifest.total_files}")
    log.info(f"Total size  : {manifest.total_size}")
    log.info(f"S3 Bucket   : {manifest.s3_bucket_name}")
    log.info(f"Job ID      : {job.id}")
    log.info(f"Status      : {job.status.split('.')[-1]}")
    log.info(f"Created at  : {job.created_at}")
    log.info(f"Started at  : {job.started_at}")
    log.info(f"Completed at: {job.completed_at}")
    log.info(f"Elapsed time: {job.elapsed_time}") 
    log.info("")

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

def human_readable_size(size_bytes: float = 0) -> str:
    """Convert a list of file paths to a human-readable size string."""
    if size_bytes == 0:
        return "0B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    i = 0
    while size_bytes >= 1024 and i < len(units) - 1:
        size_bytes = size_bytes / 1024.0
        i += 1
    return f"{size_bytes:.1f}{units[i]}"

def get_transfer_rate(size_bytes, time_str) -> str:
    # tie_str is in format '0:00:05.123456'
    time_parts = time_str.split(":")
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds = float(time_parts[2])
    total_seconds = hours * 3600 + minutes * 60 + seconds
    if total_seconds == 0:
        return "0B/s"
    rate = size_bytes / total_seconds
    return human_readable_size(int(rate)) + "/s"


class MessageHandler:
    """Class to handle job upload progress updates."""

    def __init__(self, total_files: int):
        self.total_files = total_files
        self.started_at = get_current_time()
        self.uploaded_files = 0
        self.uploaded_size_bytes = 0
    
    async def handle_update(self, message: str, completed: bool = False, 
                            uploaded_size_bytes: int = 0):
        if completed:
            self.uploaded_files += 1
            self.uploaded_size_bytes += uploaded_size_bytes

        elapsed_time = get_elapsed_time(self.started_at)  # type: ignore
        transfer_rate = get_transfer_rate(self.uploaded_size_bytes, elapsed_time)

        if message is not None:
            message = f"{message} elapsed {elapsed_time} {transfer_rate} " \
                f"[{self.uploaded_files}/{self.total_files}]"
        
        log.info(message)

def get_message_handler(total_files: int) -> MessageHandler:
    return MessageHandler(total_files)