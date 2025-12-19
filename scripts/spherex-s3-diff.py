#!/bin/env python

"""
aws s3 ls nasa-irsa-spherex/qr/level2/ --no-sign-request --recursive | tee s3.level2.ls
(cd /stage/irsa-spherex-links-ops/; find qr/level2 -ls -follow -type f| tee /stage/irsa-staff-spydi/spherex-s3-upload-work/check-sync/links.level2.ls)

aws s3 ls nasa-irsa-spherex/qr/ --no-sign-request --recursive | tee s3.qr.ls
(cd /stage/irsa-spherex-links-ops/; find qr -follow -type f -ls| tee /stage/irsa-staff-spydi/spherex-s3-upload-work/check-sync/links.qr.ls)

"""

import argparse
import asyncio
import os
import re
import subprocess
from pathlib import Path
from posixpath import dirname

from aiobotocore.config import AioConfig
from aiobotocore.paginate import Paginator
from aiobotocore.session import get_session
from botocore import UNSIGNED

qr_regex = re.compile(r".*\s(qr\S+)")

S3_PATH="nasa-irsa-spherex/qr2/"
LOCAL_PATH="/stage/irsa-spherex-links-ops/qr2"


class App:
    def __init__(self):
        self.args = None


    def main(self):
        if self.args.run_s3_ls:
            asyncio.run(self.run_s3_ls())
            return
        
        if self.args.run_local_ls:
            self.run_local_ls()
            return

        if self.args.compare:
            self.do_diff(self.args.compare[0], self.args.compare[1])
        elif self.args.s3_ls and self.args.local_ls:
            self.do_diff(self.args.s3_ls, self.args.local_ls)
        elif self.args.s3_ls or self.args.local_ls:
            raise RuntimeError("please specify both --s3-ls and --local-ls")
        else:
            raise RuntimeError("no options, please check --help")

    async def run_s3_ls(self):
        # cmd = f"aws s3 ls {S3_PATH} --no-sign-request --recursive".split()
        # output_file = os.path.abspath(self.args.run_s3_ls)
        # self.run_subprocess_tail(cmd, output_file)

        try:
            session = get_session()
            async with session.create_client(
                's3', config=AioConfig(signature_version=UNSIGNED)) as s3:
                bucket, prefix = S3_PATH.split('/', 1)

                await self.list_keys_v2(
                    s3=s3, bucket=bucket,
                    prefix=prefix)
        except KeyboardInterrupt:
            print("\nCancelling S3 operations...")
            return

    async def list_keys_v2(self, s3, bucket, prefix, max_depth=0):
        paginator: Paginator = s3.get_paginator('list_objects_v2')
        file_keys = []

        # Normalize and clean the prefix to count depth correctly
        clean_prefix = prefix.strip('/')
        prefix_depth = len(clean_prefix.split('/')) if clean_prefix else 0

        count = 0
        async for page in paginator.paginate(
            Bucket=bucket, Prefix=prefix): # type: ignore
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key'].strip('/')
                                      
                    # Calculate depth relative to prefix
                    key_depth = len(key.split('/')) - prefix_depth
                    
                    # Skip if deeper than max_depth (0 means no limit)
                    if max_depth > 0 and key_depth > max_depth:
                        continue

                    size_str = self.size_to_string(obj['Size'])
                    file_keys.append(key)
                    count += 1
                    print(f'[{count:06d}] {size_str} {key}')

    def run_local_ls(self):
        parent = Path(LOCAL_PATH).parent
        dirname = Path(LOCAL_PATH).name
        cmd = f"find {dirname} -follow -type f -ls".split()
        working_dir = str(parent)
        output_file = os.path.abspath(self.args.run_local_ls)
        self.run_subprocess_tail(cmd, output_file=output_file, working_dir=working_dir)

    @staticmethod
    def size_to_string(size: int) -> str:
        """Convert size in bytes to human readable string.

        Args:
            size: Size in bytes

        Returns:
            Human readable string with appropriate unit
        """
        if size > 1024*1024*1024:
            return f"{size/(1024*1024*1024):.2f} GB"
        elif size > 1024*1024:
            return f"{size/(1024*1024):.2f} MB"
        elif size > 1024:
            return f"{size/1024:.2f} KB"
        return f"{size:,} bytes"

    def do_diff(self, s3_ls_out: str, local_ls_out: str):
        s3_dict = {}
        s3_file_list = []

        with open(s3_ls_out) as f:
            for line in f:
                m = qr_regex.search(line)
                if m:
                    fname = m.groups()[0]
                    s3_file_list.append(fname)
                    s3_dict[fname] = 1

        print(f"=== {len(s3_file_list)} {s3_ls_out} S3 files")

        local_file_list = []
        with open(local_ls_out) as f:
            for line in f:
                m = qr_regex.search(line)
                if m:
                    fname = m.groups()[0]
                    local_file_list.append(fname)

        print(f"=== {len(local_file_list)} {local_ls_out} Local files")

        diff_file_list = []
        for f in local_file_list:
            if f not in s3_dict:
                diff_file_list.append(f)

        if not diff_file_list:
            print(f"=== Files in S3 match with Local, total {len(local_file_list)}")
            return

        print(f"=== {len(diff_file_list)} files are missing in S3 as compared to Local")
        # for f in diff_file_list:
        #     print(f)

    def run_subprocess_tail(self, command, output_file, working_dir=None):
        print(f"{command} {output_file} {working_dir}")
        original_dir = os.getcwd()

        try:
            if working_dir:
                os.chdir(working_dir)

            with open(output_file, "w") as file:
                process = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
                )
                while True:
                    line = process.stdout.readline()
                    if line == "" and process.poll() is not None:
                        break
                    if line:
                        print(line.strip())
                        file.write(line)

                # Wait for the process to finish
                process.wait()
        except Exception as e:
            raise RuntimeError(f"An error occurred: {e}")
        finally:
            os.chdir(original_dir)

    def run(self):
        self.args = self.parse_args()
        self.main()

    def parse_args(self):
        parser = argparse.ArgumentParser(
            description="Compare the file list in Links tree and S3",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
    
        parser.add_argument("--run-s3-ls", help="Run S3 ls and output to given file")
        parser.add_argument(
            "--run-local-ls", help="Run Links ls and output to given file"
        )
        parser.add_argument(
            "--compare",
            nargs=2,
            metavar=("S3_LS", "LOCAL_LS"),
            help="Compare two ls output files (S3_LS and LOCAL_LS)"
        )

        args = parser.parse_args()
        return args


if __name__ == "__main__":
    App().run()