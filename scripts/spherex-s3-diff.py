#!/bin/env python

"""
aws s3 ls nasa-irsa-spherex/qr/level2/ --no-sign-request --recursive | tee s3.level2.ls
(cd /stage/irsa-spherex-links-ops/; find qr/level2 -ls -follow -type f| tee /stage/irsa-staff-spydi/spherex-s3-upload-work/check-sync/links.level2.ls)

aws s3 ls nasa-irsa-spherex/qr/ --no-sign-request --recursive | tee s3.qr.ls
(cd /stage/irsa-spherex-links-ops/; find qr -follow -type f -ls| tee /stage/irsa-staff-spydi/spherex-s3-upload-work/check-sync/links.qr.ls)

"""

import os
from posixpath import dirname
import re
import argparse
import subprocess
from pathlib import Path

qr_regex = re.compile(r".*\s(qr\S+)")

S3_PATH="nasa-irsa-spherex/qr2/"
LOCAL_PATH="/stage/irsa-spherex-links-ops/qr2"


class App:
    def __init__(self):
        self.args = None

    def main(self):
        if self.args.run_s3_ls:
            self.run_s3_ls()
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

    def run_s3_ls(self):
        cmd = f"aws s3 ls {S3_PATH} --no-sign-request --recursive".split()
        output_file = os.path.abspath(self.args.run_s3_ls)
        self.run_subprocess_tail(cmd, output_file)

    def run_local_ls(self):
        parent = Path(LOCAL_PATH).parent
        dirname = Path(LOCAL_PATH).name
        cmd = f"find {dirname} -follow -type f -ls".split()
        working_dir = str(parent)
        output_file = os.path.abspath(self.args.run_local_ls)
        self.run_subprocess_tail(cmd, output_file=output_file, working_dir=working_dir)

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

        print(f"=== S3 files    {len(s3_file_list)} {s3_ls_out}")

        local_file_list = []
        with open(local_ls_out) as f:
            for line in f:
                m = qr_regex.search(line)
                if m:
                    fname = m.groups()[0]
                    local_file_list.append(fname)

        print(f"=== Local files {len(local_file_list)} {local_ls_out}")

        diff_file_list = []
        for f in local_file_list:
            if f not in s3_dict:
                diff_file_list.append(f)

        if not diff_file_list:
            print(f"=== Files in S3 match with Local, total {len(local_file_list)}")
            return

        print(f"=== {len(diff_file_list)} files are missing in S3 as compared to Local")
        for f in diff_file_list:
            print(f)

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