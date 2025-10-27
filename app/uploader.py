#!/bin/env python

from ast import increment_lineno
import asyncio
from functools import total_ordering
import stat
from turtle import update
from uuid import UUID
import aiobotocore.session
from aiobotocore.config import AioConfig
from botocore import UNSIGNED
from httpx import get
from loguru import logger
from pathlib import Path
from typing import Optional, Union
from datetime import datetime
from app.config import settings
from app.models import JobEntryStatus, Job, JobEntryLogRequest, JobStatus, ManifestEntry, Manifest
from app.utils import JobUploadHandler, get_current_time, get_elapsed_time, get_job_by_id, \
    get_manifest_by_id, post_entry_log, update_job, get_job_upload_handler

# use loguru's logger
from loguru import logger as log


"""
Uploader module

This module implements a lightweight in-memory manifest/job store and
the logic to upload files referenced by a manifest to S3. Key design
points:

- Concurrency: uploads are performed by a pool of async worker tasks
-Concurrency: uploads are performed by a pool of async worker tasks
    consuming an asyncio.Queue. The number of workers is controlled by
    `settings.WORKER_CONCURRENCY` so we can sustain a steady level
    of throughput without creating thousands of tasks at once.

- Throttling: `upload_file_to_s3` also uses an asyncio.Semaphore to
    provide a secondary throttle for S3 operations where appropriate.

- Multipart uploads: large files (>= MULTIPART_THRESHOLD) are uploaded
    using multipart uploads with parts of size PART_SIZE to avoid
    loading enormous files into memory.

- Anonymous S3 access: set `settings.AWS_ANON=True` to create unsigned
    S3 requests (useful for publicly readable buckets).
"""

# Tunable constants for upload behavior
MULTIPART_ENABLED = False
MULTIPART_THRESHOLD = 50 * 1024 * 1024  # 50 MB - files >= this will use multipart upload
PART_SIZE = 8 * 1024 * 1024  # 8 MB per part (>=5MB AWS minimum except last part)



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

    total_files = job.count if job.count is not None else manifest.total_files
    # log.info(f"uploading {total_files} files to S3 bucket '{bucket_name}'")

    # Build AioConfig for aiobotocore. This config controls connection
    # pooling and retry behavior for the underlying HTTP client. Tune
    # max_pool_connections to match the desired concurrency and network
    # characteristics.
    # Base AioConfig for connection pooling
    base_aio_config_kwargs = dict(
        max_pool_connections=settings.MAX_POOL_CONNECTIONS,
        retries={"max_attempts": 3},
    )

    # If anonymous (unsigned) requests requested, set signature_version to UNSIGNED
    if job.aws_unsigned:
        aio_config = AioConfig(signature_version=UNSIGNED, **base_aio_config_kwargs) # type: ignore
    else:
        aio_config = AioConfig(**base_aio_config_kwargs) # type: ignore


    # Semaphore used by individual uploads to prevent too many concurrent
    # S3 operations at once. This is a secondary throttle in addition to
    # the worker pool size and aiobotocore's connection pool.
    semaphore = asyncio.Semaphore(settings.S3_MAX_CONCURRENCY)
    # select the entries to process

    if job.count is not None and manifest.entries:
        entries = manifest.entries[
            : job.count
        ]  # the entries from the 0 to count
    else:
        entries = manifest.entries
    
    if entries is None:
        log.error(f"No manifest entries found for job {job.id}")
        return

    # Create an aiobotocore session and client. If AWS_ANON is True the
    # client will be configured to use unsigned (anonymous) requests.
    session = aiobotocore.session.get_session()
    if job.aws_unsigned:
        log.info("Creating S3 client with anonymous (unsigned) requests")

    handler = get_job_upload_handler(job.id, len(entries))

    async with session.create_client("s3", config=aio_config) as client:
        # Use an asyncio.Queue with worker tasks to sustain concurrency.
        # Producers push all work items (manifest entries) into the queue
        # quickly, then workers continuously pull work and start uploads.
        # This keeps a steady pipeline of uploads without allocating a
        # huge number of tasks at once.
        concurrency = max(1, settings.WORKER_CONCURRENCY)
        q: "asyncio.Queue[ManifestEntry]" = asyncio.Queue()

        # Producer: enqueue all entries
        for entry in entries:
            await q.put(entry)

        # Worker: consumes entries from the queue and uploads them
        async def worker(worker_id: int):
            while True:
                entry = await q.get()
                try:
                    logger.debug(f"Worker {worker_id} uploading {entry.bucket_key}")
                    ops_file = Path(manifest.ops_root_dir) / Path(entry.ops_key)
                    await upload_file_to_s3(
                        job,
                        entry,
                        ops_file,
                        bucket_name,
                        entry.bucket_key,
                        client,
                        semaphore,
                        handler=handler,
                    )
                except Exception as e:
                    logger.error(f"Worker {worker_id} failed to upload {entry.bucket_key}: {e}")
                finally:
                    q.task_done()

        # Start worker tasks. Each worker pulls entries from the queue and
        # calls the upload helper. Workers run until the queue is empty
        # and then are cancelled.
        workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]

        # Wait until all items are processed
        await q.join()

        # Cancel workers
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        # Finalize job
        await handler.handle_job_update(
            status=JobStatus.COMPLETED,
            completed_at=get_current_time(),
            message="Job completed",
        )



async def upload_file_to_s3(
    job: Job,
    entry: ManifestEntry,
    file_path: Union[str, Path],
    bucket_name: str,
    bucket_key: str,
    client,
    semaphore: asyncio.Semaphore,
    handler: JobUploadHandler,
):
    """Upload a single file to S3. For large files this uses multipart uploads.

    File I/O is offloaded to threads using asyncio.to_thread so the event loop
    isn't blocked by reads.
    """

    manifest: Manifest = job.manifest  # type: ignore
    total_files = manifest.total_files if manifest else 0

    async with semaphore:  # Limit concurrent uploads
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
            await handler.handle_job_entry_update(entry_log=entry_log)
            return

        path = Path(file_path)
        try:
            if not path.is_file():
                raise FileNotFoundError(path)
            
            size = path.stat().st_size
            if MULTIPART_ENABLED and  size >= MULTIPART_THRESHOLD and not job.aws_unsigned:
                # Large file: use multipart upload if not anonymous
                await upload_large_file_multipart(job, entry, path, bucket_name, bucket_key, client)
            else:
                # Small file: read into memory off the event loop then put_object
                body = await asyncio.to_thread(lambda: path.read_bytes())
                await client.put_object(Body=body, Bucket=bucket_name, Key=bucket_key)
        except Exception as e:
            entry_log.status = JobEntryStatus.ERROR
            entry_log.message = f"Error uploading file {path}: {e}"
            logger.error(entry_log.message)
            await handler.handle_job_entry_update(entry_log=entry_log)
            return

        entry_log.status = JobEntryStatus.COMPLETED
        entry_log.completed_at = get_current_time()
        entry_log.message = f"Uploaded {file_path}"
        await handler.handle_job_entry_update(entry_log=entry_log)


async def upload_large_file_multipart(job: Job, 
                                      entry: ManifestEntry,
                                      path: Path, bucket_name: str, bucket_key: str, client):
    """Perform a multipart upload for a large file.

    Reading file parts is done in threads to avoid blocking the event loop. Each
    part is uploaded with client.upload_part and finally complete_multipart_upload
    is called. On error, abort_multipart_upload is attempted.
    """
    # 1. initiate
    try:
        resp = await client.create_multipart_upload(Bucket=bucket_name, Key=bucket_key)
        upload_id = resp.get("UploadId")
    except Exception as e:
        logger.error(f"Failed to create multipart upload for {path}: {e}")
        raise

    parts = []
    part_number = 1
    try:
        file_size = path.stat().st_size
        offset = 0
        while offset < file_size:
            chunk_size = min(PART_SIZE, file_size - offset)

            # Read chunk in a thread
            def _read_chunk(p=path, off=offset, sz=chunk_size):
                with open(p, "rb") as f:
                    f.seek(off)
                    return f.read(sz)

            chunk = await asyncio.to_thread(_read_chunk)

            # Upload part
            upload_part_resp = await client.upload_part(
                Bucket=bucket_name,
                Key=bucket_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk,
            )

            etag = upload_part_resp.get("ETag")
            parts.append({"ETag": etag, "PartNumber": part_number})

            logger.debug(f"Uploaded part {part_number} for {path} (size={len(chunk)})")

            part_number += 1
            offset += chunk_size

        # Complete multipart upload
        await client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=bucket_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    except Exception as e:
        logger.error(f"Multipart upload failed for {path}: {e}")
        # attempt to abort
        try:
            await client.abort_multipart_upload(Bucket=bucket_name, Key=bucket_key, UploadId=upload_id)
        except Exception:
            logger.debug("Failed to abort multipart upload or upload_id missing")
        raise
