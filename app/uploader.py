#!/bin/env python

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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
    JobUploadHandler,
    get_current_time,
    get_job_by_id,
    get_job_upload_handler,
    get_manifest_by_id,
    update_job,
)

"""
Uploader module

This module implements a lightweight in-memory manifest/job store and
the logic to upload files referenced by a manifest to S3. Key design
points:

- **Pipelined Architecture**: File I/O and network uploads run in parallel
  with separate concurrency controls:
  - File readers (ThreadPoolExecutor) feed a bounded async queue
  - Network uploaders consume from the queue and upload to S3
  
- **Separate Concurrency Tuning**:
  - IO_CONCURRENCY: Controls file read parallelism (disk-bound, 32-64 for NVMe)
  - NETWORK_CONCURRENCY: Controls S3 upload parallelism (latency-bound, 100-200+)

- **Memory Management**: Bounded queue prevents OOM when reading faster than uploading

- **Anonymous S3 access**: set `settings.AWS_ANON=True` to create unsigned
    S3 requests (useful for publicly readable buckets).
"""

# --- Concurrency Settings ---
IO_CONCURRENCY = settings.IO_CONCURRENCY
NETWORK_CONCURRENCY = settings.NETWORK_CONCURRENCY
BUFFER_QUEUE_SIZE = settings.BUFFER_QUEUE_SIZE


_file_io_executor = None


def get_file_io_executor(max_workers: int = IO_CONCURRENCY) -> ThreadPoolExecutor:
    """Get or create a dedicated thread pool for file I/O operations."""
    global _file_io_executor
    if _file_io_executor is None:
        _file_io_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="file_io")
    return _file_io_executor


@dataclass
class FileReadResult:
    """Result of reading a file, ready for upload."""
    entry: ManifestEntry
    file_path: Path
    data: bytes
    error: Optional[Exception] = None



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

    job = get_job_by_id(job.id)

    log.info(f"Start time      : {job.started_at}")
    log.info(f"End   time      : {job.completed_at}")
    log.info(f"Load ID         : {manifest.load_id}")
    log.info(f"Data folders    : {', '.join(manifest.data_folders)}")
    log.info(f"Total files     : {manifest.total_files}")
    log.info(f"Requested Count : {job.count}")
    log.info(
        f"Uploaded {job.uploaded_files} files, {manifest.total_size} in time {job.elapsed_time}"
    )


async def upload_to_s3_in_batch(job: Job, bucket_name: str):

    if not job.manifest or not job.manifest.entries:
        log.error(f"Job {job.id} has no manifest or manifest entries")
        return
    
    manifest : Manifest = job.manifest

    # Build AioConfig for aiobotocore. Tune max_pool_connections to match
    # network concurrency for optimal connection reuse.
    base_aio_config_kwargs = dict(
        max_pool_connections=NETWORK_CONCURRENCY + 10,  # Slightly above concurrency
        retries={"max_attempts": 3, "mode": "adaptive"},
        connect_timeout=10,
        read_timeout=60,
    )

    # If anonymous (unsigned) requests requested, set signature_version to UNSIGNED
    if job.aws_unsigned:
        aio_config = AioConfig(signature_version=UNSIGNED, **base_aio_config_kwargs) # type: ignore
    else:
        aio_config = AioConfig(**base_aio_config_kwargs) # type: ignore

    # select the entries to process
    if job.count is not None and manifest.entries:
        entries = manifest.entries[: job.count]
    else:
        entries = manifest.entries
    
    if entries is None:
        log.error(f"No manifest entries found for job {job.id}")
        return

    # Create an aiobotocore session and client.
    session = aiobotocore.session.get_session()
    if job.aws_unsigned:
        log.info("Creating S3 client with anonymous (unsigned) requests")

    handler = get_job_upload_handler(job.id, len(entries))

    # Initialize file I/O executor
    io_executor = get_file_io_executor(max_workers=IO_CONCURRENCY)

    log.info(f"Starting pipelined upload: {len(entries)} files")
    log.info(f"  IO concurrency: {IO_CONCURRENCY} (file reads)")
    log.info(f"  Network concurrency: {NETWORK_CONCURRENCY} (S3 uploads)")
    log.info(f"  Buffer queue size: {BUFFER_QUEUE_SIZE}")

    async with session.create_client("s3", config=aio_config) as client:
        # Pipelined architecture with separate stages:
        # Stage 1: File readers (ThreadPool) -> bounded queue
        # Stage 2: Network uploaders (async) <- bounded queue
        
        # Bounded queue prevents memory exhaustion when reads outpace uploads
        file_queue: "asyncio.Queue[Optional[FileReadResult]]" = asyncio.Queue(maxsize=BUFFER_QUEUE_SIZE)
        
        # Semaphore for I/O concurrency (controls ThreadPool submission rate)
        io_semaphore = asyncio.Semaphore(IO_CONCURRENCY)
        
        # Track completion
        read_complete = asyncio.Event()
        
        async def read_file_async(entry: ManifestEntry) -> FileReadResult:
            """Read a single file using thread pool."""
            file_path = Path(manifest.ops_root_dir) / Path(entry.ops_key)
            try:
                # Offload blocking I/O to thread pool
                data = await asyncio.get_event_loop().run_in_executor(
                    io_executor, 
                    file_path.read_bytes
                )
                return FileReadResult(entry=entry, file_path=file_path, data=data)
            except Exception as e:
                return FileReadResult(entry=entry, file_path=file_path, data=b'', error=e)

        async def file_reader_producer():
            """Producer: reads files and puts results into queue."""
            async def read_and_enqueue(entry: ManifestEntry):
                async with io_semaphore:
                    result = await read_file_async(entry)
                    await file_queue.put(result)
            
            # Create all read tasks but limit concurrency via semaphore
            tasks = [asyncio.create_task(read_and_enqueue(entry)) for entry in entries]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Signal completion to uploaders
            for _ in range(NETWORK_CONCURRENCY):
                await file_queue.put(None)  # Sentinel values
            read_complete.set()

        async def network_uploader_worker(worker_id: int):
            """Consumer: uploads files from queue to S3."""
            while True:
                result = await file_queue.get()
                
                # Check for sentinel (end of work)
                if result is None:
                    file_queue.task_done()
                    break
                
                try:
                    await upload_file_data_to_s3(
                        job=job,
                        entry=result.entry,
                        file_path=result.file_path,
                        file_data=result.data,
                        read_error=result.error,
                        bucket_name=bucket_name,
                        client=client,
                        handler=handler,
                    )
                except Exception as e:
                    log.error(f"Worker {worker_id} upload error for {result.entry.bucket_key}: {e}")
                finally:
                    file_queue.task_done()

        # Start producer (file readers)
        producer_task = asyncio.create_task(file_reader_producer())
        
        # Start consumer workers (network uploaders)
        upload_workers = [
            asyncio.create_task(network_uploader_worker(i)) 
            for i in range(NETWORK_CONCURRENCY)
        ]

        # Wait for all work to complete
        await producer_task
        await asyncio.gather(*upload_workers, return_exceptions=True)

        # Finalize job
        await handler.handle_job_update(
            status=JobStatus.COMPLETED,
            completed_at=get_current_time(),
            message="Job completed",
        )

async def upload_file_data_to_s3(
    job: Job,
    entry: ManifestEntry,
    file_path: Path,
    file_data: bytes,
    read_error: Optional[Exception],
    bucket_name: str,
    client,
    handler: JobUploadHandler,
):
    """Upload pre-read file data to S3.
    
    This function receives already-read file data from the pipeline,
    avoiding blocking I/O in the upload path. For large files (>5MB),
    consider implementing multipart uploads.
    """
    entry_log = JobEntryLogRequest(
        job_id=job.id,
        entry_id=entry.id,
        status=JobEntryStatus.STARTED,
        started_at=get_current_time(),
    )   
    
    # Handle read errors from the producer stage
    if read_error is not None:
        entry_log.status = JobEntryStatus.ERROR
        entry_log.message = f"Error reading file {file_path}: {read_error}"
        log.error(entry_log.message)
        await handler.handle_job_entry_update(entry_log=entry_log)
        return
    
    if job.mock:
        entry_log.status = JobEntryStatus.COMPLETED
        entry_log.completed_at = get_current_time()
        entry_log.message = f"Uploaded {file_path} (mock)"
        await handler.handle_job_entry_update(entry_log=entry_log)
        return

    try:
        # Data is already read - just upload
        await client.put_object(Body=file_data, Bucket=bucket_name, Key=entry.bucket_key)
    except Exception as e:
        entry_log.status = JobEntryStatus.ERROR
        entry_log.message = f"Error uploading file {file_path}: {e}"
        log.error(entry_log.message)
        await handler.handle_job_entry_update(entry_log=entry_log)
        return

    entry_log.status = JobEntryStatus.COMPLETED
    entry_log.completed_at = get_current_time()
    entry_log.message = f"Uploaded {file_path}"
    await handler.handle_job_entry_update(entry_log=entry_log)
