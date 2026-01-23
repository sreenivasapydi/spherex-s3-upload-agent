# spherex-s3-upload-agent

# Overview

This repository contains scripts to manage manifests and jobs for uploading data to the S3 bucket `nasa-irsa-spherex`. The scripts allow you to create and list manifests, create and manage jobs, and check the synchronization status between S3 and local filesystem.

A manifest represents a collection of files to be uploaded to S3, identified by a unique Load ID. A job represents the process of uploading the files specified in a manifest to S3. 

Spherex pipeline send a Slack notification on #irsa-ingest-notify channel when a Load ID is ready for IRSA.
```
SPHEREx IRSA-qr2-2026_024_20260120T104438 RECEIVED at 2026-01-20 10:44:38
```

This request is processed by another script that sends a Slack notification to #irsa-ingest-ops channel with the Load ID and the ingestion status "INGESTED OK" when the files have been ingested and are ready for upload to S3.
```
SPHEREx IRSA-qr2-2026_024_20260120T104438 INGESTED OK at 2026-01-20 20:56:51
```

On receiving this notice, you can then create a manifest and a job for the specified Load ID using the scripts provided in this repository.

Note: AWS credentials must be configured in your environment to allow the scripts to interact with the S3 bucket. 

# Job Statuses

A jobs status can be one of the following: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`. On create, the job status is set to `PENDING`. The job status will be updated to `RUNNING` when the job starts, and to `COMPLETED`, `FAILED`, or `CANCELLED` when the job ends.

A job can be run only if its status is `PENDING`. Running a job will change its status to `RUNNING`. Once the job is completed, its status will be updated to `COMPLETED` or `FAILED` based on the outcome. Make sure to create a job before attempting to run it.


# Run steps

## Create the manifest for a given Load ID

```
uv run python scripts/spherex-s3-manifests.py --load-id <LOAD_ID> --create
```
## List manifests, optionally filtered by Load ID

```
uv run python scripts/spherex-s3-manifests.py [--load-id <LOAD_ID>] --list
```

## Create a job for a given Load ID

```
uv run python scripts/spherex-s3-jobs.py --load-id <LOAD_ID> --create
```

## List jobs, optionally filtered by Load ID

```
uv run python scripts/spherex-s3-jobs.py [--load-id <LOAD_ID>] --list
```

## Run a job for a given Load ID

```
uv run python scripts/spherex-s3-jobs.py --load-id <LOAD_ID> --run
```

## Cancel a job for a given Load ID

```
uv run python scripts/spherex-s3-jobs.py --load-id <LOAD_ID> --cancel
```
## Generate a report for a given Load ID

```
uv run python scripts/spherex-s3-jobs.py --load-id <LOAD_ID> --report
```


# Check sync between S3 and local links ops directory
This repository contains scripts to check the sync status between S3 bucket `nasa-irsa-spherex` and local files in `/stage/irsa-spherex-links-ops/`. The scripts help identify any discrepancies between the files listed in S3 and the local ops directory.


## Generate a file listing from S3 

```
uv run python scripts/spherex-s3-diff.py --run-s3-ls qr2.s3.ls
```
# Generate a file listing from local ops directory

```
uv run python scripts/spherex-s3-diff.py --run-local-ls qr2.local.ls
```

## Compare the two listings to find discrepancies

```
uv run python scripts/spherex-s3-diff.py --compare qr2.s3.ls qr2.local.ls
```

