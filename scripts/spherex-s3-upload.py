import argparse
import logging
import os
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
import httpx
from enum import Enum


sys.path.append(Path(__file__).resolve().parent.parent.as_posix())


from loguru import logger


SERVICE_URL = os.getenv("SPHEREX_SERVICE_URL", "http://irsaspherexingest:8080") 
WORK_DIR = "/stage/irsa-staff-spydi/spherex-s3-upload-work"


class App:
    def __init__(self):
        self.args = None
        self.load_id = None
        self.service_url = None

    def main(self):
        self.load_id = self.args.load_id
        self.service_url = self.args.service_url

        if self.args.create:
            self.create_manifest(self.args.load_id, self.args.manifest_file)
        elif self.args.query:
            self.query_manifest(load_id=self.load_id)
        elif self.args.submit:
            self.submit_manifest(self.load_id, mock=self.args.mock, count=self.args.count)

    def create_manifest(self, load_id=None, manifest_file=None):

        if not load_id:
            raise ValueError("Error: --load_id is required for creating a manifest")

        print(f"Creating manifest from {load_id}")
    
        url = f"{self.service_url}/manifests"
        print(f"Posting to {url}")
        payload = {
            "load_id": load_id,
            "manifest_file": manifest_file
        }
        print(f"Payload: ")
        print(json.dumps(payload, indent=2))

        with httpx.Client(timeout=60) as client:
            r = client.post(url, json=payload)
            try:
                r.raise_for_status()
            except httpx.HTTPStatusError:
                # print response detail if available
                try:
                    detail = r.json()
                except Exception:
                    detail = r.text
                print(f"Error response detail: {detail}")
                raise
        data = r.json()
        print(json.dumps(data, indent=2))

    def query_manifest(self, load_id=None, latest=False):
        print(f"Querying manifests from {self.service_url}")
        url = f"{self.service_url}/manifests"
        params = {'latest' : latest}
        if load_id:
            params['load_id'] = load_id
        with httpx.Client(timeout=60) as client:
            r = client.get(url, params=params)
            r.raise_for_status()

        data = r.json()
        for record in data:
            del record['entries']
            print(json.dumps(record, indent=2))
        return data

    def submit_manifest(self, load_id=None, count: Optional[int] = None, mock=False):

        if not load_id:
            raise ValueError("Error: --load_id is required for submitting a job")

        print(f"Submitting job for load ID {load_id}")

        url = f"{self.service_url}/manifests"
        params = {'latest' : True}
        if load_id:
            params['load_id'] = load_id
        with httpx.Client(timeout=60) as client:
            r = client.get(url, params=params)
            r.raise_for_status()

        data = r.json()
        if not data:
            raise ValueError("No manifest found")
        
        manifest = data[0]
        print(f"Using manifest ID {manifest['id']}")

        url = f"{self.service_url}/jobs"
        payload = {
            "manifest_id" : manifest['id'],
            "mock"  : mock,
            "count" : count,
            "aws_unsigned": self.args.aws_unsigned
        }

        with httpx.Client(timeout=60) as client:
            r = client.post(url, json=payload)
            r.raise_for_status()
        data = r.json()
    
        del data['manifest']
        print("Job submitted:")
        print(json.dumps(data, indent=2))
    


    def parse_args(self):
        parser = argparse.ArgumentParser(
            description="Upload files to S3 based on a manifest file",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--load-id", help="Load ID")
        parser.add_argument("--manifest-file", help="Path to manifest file")
        parser.add_argument("--manifest-id", help="Manifest ID")        
        parser.add_argument(
            "--count", type=int, help="file count, process the given number of files"
        )
        parser.add_argument("--service-url", 
                            default=SERVICE_URL,
                            required=False,
                            help="URL of the manifest service")
        parser.add_argument("--create", action="store_true")
        parser.add_argument("--query", action="store_true")
        parser.add_argument("--submit", action="store_true")
        parser.add_argument("--mock", action="store_true")
        parser.add_argument("--aws-unsigned", action="store_true") 


        args = parser.parse_args()
        return args

    def run(self):
        self.args = self.parse_args()

        self.main()




class LogLevels(str, Enum):
    """Enumeration for log levels."""

    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    DEBUG = "DEBUG"

    def __str__(self):
        return self.value


def configure_logging(level: LogLevels):
    """Configure loguru logger and intercept stdlib logging."""
    log_level = str(level).upper()
    log_levels = [lvl.value for lvl in LogLevels]
    if log_level not in log_levels:
        log_level = LogLevels.ERROR

    # remove any default handlers added by loguru
    logger.remove()

    # add file sink
    log_filename = get_log_filename()

    # rotation and retention are sensible defaults
    logger.add(
        log_filename,
        level=log_level,
        rotation="20 MB",
        retention="10 days",
        enqueue=True,
        # backtrace=True,
        # diagnose=True,
    )

    # add console sink
    logger.add(sys.stdout, level=log_level, colorize=True)
    # Keep stdlib logging configuration minimal — don't intercept handlers.
    # This lets 3rd-party libraries continue to use the stdlib logging while
    # the app uses loguru directly.
    logging.basicConfig(level=log_level)

def get_log_filename():
    work_dir = WORK_DIR
    log_dir = Path(f"{work_dir}/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_filename = f"{log_dir}/logfile-{timestamp}.log"
    print(f"Log file {log_filename}")
    return log_filename





# Run the async program
if __name__ == "__main__":
    App().run()
