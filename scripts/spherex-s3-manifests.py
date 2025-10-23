import argparse
import os
import sys
import json
from pathlib import Path
from typing import Optional
import httpx
from loguru import logger as log

sys.path.append(Path(__file__).resolve().parent.parent.as_posix())

from app.utils import get_manifest_by_load_id
from app.config import settings


class App:
    def __init__(self):
        self.args = None
        self.load_id = None
        self.service_url = settings.SPHEREX_UPLOAD_SERVICE_URL

    def main(self):
        self.load_id = self.args.load_id # pyright: ignore[reportOptionalMemberAccess]

        if self.args.create:
            self.create_manifest(self.args.load_id, self.args.manifest_file)
        elif self.args.query:
            self.query_manifest(load_id=self.load_id)
        else:
            log.error("No action specified. Use --create, or --query.")


    def create_manifest(self, load_id=None, manifest_file=None):
        if not load_id:
            raise ValueError("Error: --load_id is required for creating a manifest")

        log.info(f"Creating manifest from {load_id}")
    
        url = f"{self.service_url}/manifests"
        log.info(f"Posting to {url}")
        payload = {
            "load_id": load_id,
            "manifest_file": manifest_file
        }

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
                log.error(f"Error response detail: {detail}")
                raise
        data = r.json()
        log.info(f"Manifest created successfully: {data}")
        return data

    def query_manifest(self, load_id: str):
        log.info(f"Querying manifests from {self.service_url}")
        
        try:
            manifest = get_manifest_by_load_id(load_id)
        except ValueError as e:
            log.error(f"Error fetching manifest: {e}")
            return
        print("Manifest found:")
        print(json.dumps(manifest, indent=2))
    

    def parse_args(self):
        parser = argparse.ArgumentParser(
            description="Upload files to S3 based on a manifest file",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--load-id", required=True, help="Load ID")
        parser.add_argument("--manifest-file", help="Path to manifest file")
        parser.add_argument("--create", action="store_true")
        parser.add_argument("--query", action="store_true")

        args = parser.parse_args()
        return args

    def run(self):
        self.args = self.parse_args()

        self.main()


# Run the async program
if __name__ == "__main__":
    App().run()
