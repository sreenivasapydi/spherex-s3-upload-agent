import argparse
import sys
from pathlib import Path
from typing import Optional
import httpx
from uuid import UUID
from loguru import logger as log
import asyncio

sys.path.append(Path(__file__).resolve().parent.parent.as_posix())

from app.models import JobStatus

from app.config import settings
from app import utils, uploader



class App:
    def __init__(self):
        self.args: argparse.Namespace
        self.service_url = settings.SPHEREX_UPLOAD_SERVICE_URL

    def main(self):

        manifest_id = self.args.manifest_id
        load_id = self.args.load_id

        if self.args.create:
            self.create_job(manifest_id=manifest_id, load_id=load_id, 
                            mock=self.args.mock, count=self.args.count)
        elif self.args.query:
            self.query_jobs(manifest_id=manifest_id, load_id=load_id)
        elif self.args.run:
            self.run_job(manifest_id=manifest_id, load_id=load_id, mock=self.args.mock, count=self.args.count)
        else:
            log.error("No action specified. Use --create, --query, or --submit.")


    def create_job(self, 
                   load_id: Optional[str] = None, 
                   manifest_id: Optional[UUID] = None,
                   mock: bool = False,
                   count: Optional[int] = None):
        
        manifest = utils.find_manifest(load_id=load_id, manifest_id=manifest_id)
    
        if manifest is None:
            raise ValueError("Error: cannot find the manifest for the given load_id or manifest_id")

        log.info(f"Using manifest ID {manifest.id}")
        try:
            job = utils.create_job(manifest_id=manifest.id, mock=mock, count=count)
        except httpx.HTTPStatusError as exc:
            log.error(f"failed to create job: {exc.response.json()}")
            return

        log.info(f"Created job:")
        log.info(job.model_dump_json(indent=2))



    def query_jobs(self, manifest_id: Optional[UUID] = None, load_id: Optional[str] = None):
        if not manifest_id and not load_id:
            raise ValueError("Error: --manifest_id or --load_id is required for querying jobs")

        if self.args.all:
            jobs = utils.get_jobs(manifest_id=manifest_id, load_id=load_id)
        else:
            jobs = utils.get_pending_jobs(manifest_id=manifest_id, load_id=load_id)

        if not jobs:
            log.info("No jobs found")
            return
        for job in jobs:
            # Ensure UUIDs and other special types are JSON-serializable
            log.info(job.model_dump_json(indent=2))

    def run_job(self, 
                load_id: Optional[str] = None, 
                manifest_id: Optional[UUID] = None, 
                count: Optional[int] = None, 
                mock: bool = False):

        if not manifest_id and not load_id:
            raise ValueError("Error: --manifest_id or --load_id is required for querying jobs")
        
        try:
            jobs = utils.get_pending_jobs(load_id=load_id, manifest_id=manifest_id)
        except ValueError as e:
            log.error(f"Error finding job to run: {e}")
            return
        
        if not jobs:
            log.info("No pending jobs found")
            return
        
        if len(jobs) > 1:
            log.error(f"Multiple pending jobs found, cannot proceed: {[job.id for job in jobs]}")
            return

        job = jobs[0]
        log.info(f"Running job ID {job.id} for manifest ID {job.manifest_id}, load ID {load_id}, count {count}, mock {mock}")

        asyncio.run(uploader.run_job(job))

    

    def parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Upload files to S3 based on a manifest file",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--load-id", help="Load ID")
        parser.add_argument("--manifest-id", help="Manifest ID")
        parser.add_argument(
            "--count", type=int, help="file count, process the given number of files"
        )
        parser.add_argument("--service-url", 
                            default=self.service_url,
                            required=False,
                            help="URL of the manifest service")
        parser.add_argument("--create", action="store_true")
        parser.add_argument("--query", action="store_true")
        parser.add_argument("--all", action="store_true")
        parser.add_argument("--pending", action="store_true")
        parser.add_argument("--run", action="store_true")
        parser.add_argument("--mock", action="store_true")
        parser.add_argument("--aws-unsigned", action="store_true") 


        args = parser.parse_args()
        return args

    def config_logger(self):
        # Remove default logger
        log.remove()
            # Custom format for info messages
        def info_filter(record):
            return record["level"].name == "INFO"

        log.add(sys.stdout, format="{message}", filter=info_filter, level="INFO")
        # Custom format for error messages in red
        def error_filter(record):
            return record["level"].name == "ERROR"
        
        log.add(sys.stderr, format="<red>ERROR {message}</red>", filter=error_filter, level="ERROR")

    def run(self):
        self.args : argparse.Namespace = self.parse_args()
        self.config_logger()
        self.main()


# Run the async program
if __name__ == "__main__":
    App().run()
