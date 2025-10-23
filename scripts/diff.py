#!/bin/env python

"""
aws s3 ls nasa-irsa-spherex/qr/level2/ --no-sign-request --recursive | tee s3.level2.ls
(cd /stage/irsa-spherex-links-ops/; find qr/level2 -ls -follow -type f| tee /stage/irsa-staff-spydi/spherex-s3-upload-work/check-sync/links.level2.ls)

aws s3 ls nasa-irsa-spherex/qr/ --no-sign-request --recursive | tee s3.qr.ls
(cd /stage/irsa-spherex-links-ops/; find qr -follow -type f -ls| tee /stage/irsa-staff-spydi/spherex-s3-upload-work/check-sync/links.qr.ls)

"""

import os
import re
import argparse
import subprocess

qr_regex = re.compile(r".*\s(qr\S+)")


class App:
    def __init__(self):
        self.args = None

    def main(self):
        if self.args.run_s3_ls:
            self.run_s3_ls()

        if self.args.run_links_ls:
            self.run_links_ls()

        if self.args.s3_ls and self.args.links_ls:
            self.do_diff(self.args.s3_ls, self.args.links_ls)
        elif self.args.s3_ls or self.args.links_ls:
            raise RuntimeError("please specify both --s3-ls and --links-ls")
        else:
            raise RuntimeError("no options, please check --help")

    def run_s3_ls(self):
        cmd = "aws s3 ls nasa-irsa-spherex/qr/ --no-sign-request --recursive".split()
        output_file = os.path.abspath(self.args.run_s3_ls)
        self.run_subprocess_tail(cmd, output_file)

    def run_links_ls(self):
        cmd = "find qr -follow -type f -ls".split()
        working_dir = "/stage/irsa-spherex-links-ops/"
        output_file = os.path.abspath(self.args.run_links_ls)
        self.run_subprocess_tail(cmd, output_file=output_file, working_dir=working_dir)

    def do_diff(self, s3_ls_out: str, links_ls_out: str):
        s3_ls_out = self.args.s3_ls
        links_ls_out = self.args.links_ls

        s3_dict = {}
        s3_file_list = []

        with open(s3_ls_out) as f:
            for line in f:
                m = qr_regex.search(line)
                if m:
                    fname = m.groups()[0]
                    s3_file_list.append(fname)
                    s3_dict[fname] = 1

        print(f"=== S3 files    {len(s3_file_list)}")

        links_file_list = []
        with open(links_ls_out) as f:
            for line in f:
                m = qr_regex.search(line)
                if m:
                    fname = m.groups()[0]
                    links_file_list.append(fname)

        print(f"=== Links files {len(links_file_list)}")

        diff_file_list = []
        for f in links_file_list:
            if f not in s3_dict:
                diff_file_list.append(f)

        if not diff_file_list:
            print(f"=== Files in S3 match with Links, total {len(links_file_list)}")
            return

        print(f"=== {len(diff_file_list)} files are missing in S3 as compared to Links")
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

        parser.add_argument("--s3-ls", help="output of S3 ls")
        parser.add_argument("--links-ls", help="output of Links ls")
        parser.add_argument("--run-s3-ls", help="Run S3 ls and output to given file")
        parser.add_argument(
            "--run-links-ls", help="Run Links ls and output to given file"
        )

        args = parser.parse_args()
        return args


def run():
    App().run()
