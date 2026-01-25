#!/bin/env python

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import aiobotocore.session
from aiobotocore.config import AioConfig
from botocore import UNSIGNED

# use loguru's logger
from loguru import logger as log

from app.config import settings
from app.models import (
    Job,
    JobEntryLogRequest,
    JobEntryStatus,
    JobStatus,
    Manifest,
    ManifestEntry,
)
from app.utils import (
    MessageHandler,
    get_current_time,
    get_elapsed_time,
    get_manifest_by_id,
    get_message_handler,
    get_transfer_rate,
    human_readable_size,
    post_entry_log,
    update_job,
)

"""
Uploader module

This module uploads files referenced by a manifest to S3 using a simple
worker queue pattern optimized for medium-sized files (6-80MB).

Key design points:
- **Worker Queue**: Simple asyncio Queue with N worker tasks
- **Single concurrency dimension**: worker_concurrency controls parallelism
- **File I/O in thread pool**: Avoids blocking the event loop
- **Anonymous S3 access**: set `settings.AWS_UNSIGNED=True` for unsigned requests
"""

# --- Concurrency Settings (from config) ---
WORKER_CONCURRENCY = settings.NETWORK_CONCURRENCY  # Number of concurrent upload workers
S3_MAX_CONCURRENCY = settings.NETWORK_CONCURRENCY  # S3 connection pool size
MAX_POOL_CONNECTIONS = settings.NETWORK_CONCURRENCY + 10  # Connection pool buffer

# File I/O thread pool
_file_io_executor: ThreadPoolExecutor | None = None


def get_file_io_executor(max_workers: int = 64) -> ThreadPoolExecutor:
    """Get or create a dedicated thread pool for file I/O operations."""
    global _file_io_executor
    if _file_io_executor is None:
        _file_io_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="file_io")
    return _file_io_executor


async def run_job(job: Job):
    """Run the job to upload files to S3."""

    manifest = get_manifest_by_id(job.manifest_id)
    job.manifest = manifest

    job.status = JobStatus.RUNNING
    job.updated_at = job.started_at = get_current_time()
    await update_job(
        job.id,
        status=job.status,
        started_at=job.started_at,
    )

    await upload_to_s3_in_batch(job, settings.S3_BUCKET_NAME)

    transfer_rate = get_transfer_rate(
        job.uploaded_size_bytes, job.elapsed_time or "0:00:00"
    )
    uploaded_size_hr = human_readable_size(job.uploaded_size_bytes)

    log.info(f"Load ID           : {manifest.load_id}")
    log.info(f"Data folders      : {', '.join(manifest.data_folders)}")
    log.info(f"Total files       : {manifest.total_files}")
    log.info(f"Requested Count   : {job.count}")
    log.info(f"Worker concurrency: {WORKER_CONCURRENCY}")
    log.info(f"Start time        : {job.started_at}")
    log.info(f"End   time        : {job.completed_at}")
    log.info(
        f"Uploaded {job.uploaded_files} files, {uploaded_size_hr} "
        f"in time {job.elapsed_time} at {transfer_rate} "
        f"(worker_concurrency={WORKER_CONCURRENCY})"
    )


async def upload_to_s3_in_batch(job: Job, bucket_name: str):
    if not job.manifest or not job.manifest.entries:
        log.error(f"Job {job.id} has no manifest or manifest entries")
        return
    
    manifest: Manifest = job.manifest

    # Calculate optimal pool size
    effective_concurrency = max(
        S3_MAX_CONCURRENCY,
        WORKER_CONCURRENCY * 2,
        MAX_POOL_CONNECTIONS
    )

    # Build AioConfig for aiobotocore with optimized settings
    base_aio_config_kwargs = dict(
        max_pool_connections=effective_concurrency,
        retries={"max_attempts": 3, "mode": "adaptive"},
        connect_timeout=10,
        read_timeout=60,
        tcp_keepalive=True,
    )

    # If anonymous (unsigned) requests requested, set signature_version to UNSIGNED
    if job.aws_unsigned:
        aio_config = AioConfig(signature_version=UNSIGNED, **base_aio_config_kwargs)  # type: ignore
    else:
        aio_config = AioConfig(**base_aio_config_kwargs)  # type: ignore

    log.info(f"Connection pool size: {effective_concurrency}")

    # Select the entries to process
    if job.count is not None and manifest.entries:
        entries = manifest.entries[: job.count]
    else:
        entries = manifest.entries
    
    if entries is None:
        log.error(f"No manifest entries found for job {job.id}")
        return

    # Create an aiobotocore session and client
    session = aiobotocore.session.get_session()
    if job.aws_unsigned:
        log.info("Creating S3 client with anonymous (unsigned) requests")

    handler = get_message_handler(len(entries))

    # Initialize file I/O executor with workers matching concurrency
    io_executor = get_file_io_executor(max_workers=WORKER_CONCURRENCY * 2)

    log.info(f"Starting upload: {len(entries)} files")
    log.info(f"  Worker concurrency: {WORKER_CONCURRENCY}")

    async with session.create_client("s3", config=aio_config) as client:
        # Simple worker queue pattern
        concurrency = max(1, WORKER_CONCURRENCY)
        q: "asyncio.Queue[ManifestEntry]" = asyncio.Queue()

        # Producer: enqueue all entries
        for entry in entries:
            await q.put(entry)

        # Worker: consumes entries from the queue and uploads them
        async def worker(worker_id: int):
            while True:
                entry = await q.get()
                try:
                    log.debug(f"Worker {worker_id} uploading {entry.bucket_key}")
                    await upload_file_to_s3(
                        job=job,
                        entry=entry,
                        bucket_name=bucket_name,
                        client=client,
                        handler=handler,
                        io_executor=io_executor,
                    )
                except Exception as e:
                    log.error(f"Worker {worker_id} failed to upload {entry.bucket_key}: {e}")
                finally:
                    q.task_done()

        # Start worker tasks
        workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]

        # Wait until all items are processed
        await q.join()

        # Cancel workers
        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

    # Update job completion info
    job.completed_at = get_current_time()
    job.elapsed_time = get_elapsed_time(job.started_at, job.completed_at)  # type: ignore
    job.uploaded_files = handler.uploaded_files

    # Finalize job status
    job.status = JobStatus.COMPLETED

    await update_job(
        job.id,
        status=JobStatus.COMPLETED,
        message="Job completed",
        completed_at=job.completed_at,
        uploaded_files=job.uploaded_files
    )
    
    log.info("Job upload completed")


async def upload_file_to_s3(
    job: Job,
    entry: ManifestEntry,
    bucket_name: str,
    client,
    handler: MessageHandler,
    io_executor: ThreadPoolExecutor,
):
    """Upload a single file to S3.
    
    File I/O is offloaded to a dedicated thread pool to avoid blocking the event loop.
    """
    manifest: Manifest = job.manifest  # type: ignore
    file_path = Path(manifest.ops_root_dir) / Path(entry.ops_key)

    entry_log = JobEntryLogRequest(
        job_id=job.id,
        entry_id=entry.id,
        status=JobEntryStatus.STARTED,
        started_at=get_current_time(),
    )

    if job.mock:
        entry_log.status = JobEntryStatus.COMPLETED
        entry_log.completed_at = get_current_time()
        entry_log.message = f"Uploaded {file_path} (mock)"
        await post_entry_log(entry_log)
        await handler.handle_update(message=entry_log.message, completed=True)
        return

    try:
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        
        size = file_path.stat().st_size

        # Read file in dedicated I/O thread pool
        loop = asyncio.get_event_loop()
        body = await loop.run_in_executor(io_executor, file_path.read_bytes)

        # Upload to S3
        await client.put_object(Body=body, Bucket=bucket_name, Key=entry.bucket_key)

    except Exception as e:
        entry_log.status = JobEntryStatus.ERROR
        entry_log.message = f"Error uploading file {file_path}: {e}"
        log.error(entry_log.message)
        await post_entry_log(entry_log)
        await handler.handle_update(message=entry_log.message)
        return

    # Track uploaded bytes
    job.uploaded_files += 1
    job.uploaded_size_bytes += size
    entry_log.uploaded_size_bytes = size

    entry_log.status = JobEntryStatus.COMPLETED
    entry_log.completed_at = get_current_time()
    entry_log.message = f"Uploaded {file_path}"
    await post_entry_log(entry_log)
    await handler.handle_update(
        message=entry_log.message, completed=True, uploaded_size_bytes=size
    )

