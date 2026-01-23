import argparse
import sys
from pathlib import Path

from loguru import logger as log

sys.path.append(Path(__file__).resolve().parent.parent.as_posix())


from app.config import settings
from app.utils import create_manifest, find_manifest, list_manifests


class App:
    def __init__(self):
        self.args : argparse.Namespace
        self.load_id = None
        self.service_url = settings.SPHEREX_UPLOAD_SERVICE_URL

    def main(self):
        self.load_id = self.args.load_id # pyright: ignore[reportOptionalMemberAccess]

        if self.args.create:
            self.create_manifest(self.args.load_id, self.args.manifest_file)
        elif self.args.list or self.load_id:
            self.query_manifests(load_id=self.load_id)
        else:
            log.error("No action specified. Use --create or --list.")


    def create_manifest(self, load_id: str | None = None, manifest_file: str | None = None):

        if load_id:
            try:
                manifest = find_manifest(load_id=load_id)
                if manifest:
                    log.info(f"Manifest already exists for load_id {load_id}: {manifest.id}")
                    return manifest
            except ValueError:
                pass
        
        if not load_id and not manifest_file:
            raise ValueError("Error: --load-id or --manifest-file is required for creating a manifest")

        log.info(f"Creating manifest from {load_id}")
        manifest = create_manifest(load_id, manifest_file)         # type: ignore

        log.info("Created manifest:")
        log.info(manifest.model_dump_json(indent=2, exclude_none=True))
    
        return

    def query_manifests(self, load_id: str | None = None):
        log.info(f"Querying manifests from {self.service_url}")
        
        try:
            manifests = list_manifests(load_id)
        except ValueError as e:
            log.error(f"Error fetching manifests: {e}")
            return
        print("Manifest found:")
        for manifest in manifests:
            print(manifest.model_dump_json(indent=2, exclude_none=True))
    

    def parse_args(self):
        parser = argparse.ArgumentParser(
            description="Upload files to S3 based on a manifest file",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--load-id", help="Load ID")
        parser.add_argument("--manifest-file", help="Path to manifest file")
        parser.add_argument("--create", action="store_true")
        parser.add_argument('--list', action='store_true', help='List manifests')

        args = parser.parse_args()
        return args

    def run(self):
        self.args = self.parse_args()

        self.main()


# Run the async program
if __name__ == "__main__":
    App().run()
