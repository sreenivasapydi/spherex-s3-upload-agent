# spherex-s3-upload-agent



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

