#!/usr/bin/env python3.9
# run whisper on HPC for some local files.

import argparse
from hpc_client import HPCClient
import logging
from pathlib import Path
import time
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', default=False, action="store_true", help="Turn on debugging")
    parser.add_argument('infile', nargs='+', type=Path, help="Input files")
    parser.add_argument('outdir', type=Path, help="Output directory")
    parser.add_argument('--model', default='medium', choices=['tiny', 'base', 'small', 'medium', 'large'], help="Whisper model")
    parser.add_argument("--hpcuser", type=str, default=None, help="User on HPC")
    parser.add_argument("--hpchost", type=str, default="bigred200.uits.iu.edu", help="HPC Host")
    parser.add_argument("--hpcscript", type=str, default="iu_hpc_processing/hpc_service.py")
    parser.add_argument("--scpuser", type=str, default=None, help="SCP User")
    parser.add_argument("--scphost", type=str, default=None, help="SCP File Host")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    if not args.outdir.is_dir():
        logging.error("Output directory must be a directory")
        exit(1)

    files = []
    tasklist = []
    for ifile in args.infile:
        p = {'infile': str(ifile.absolute()),
             'outfile': str((args.outdir / ifile.name).with_name(ifile.name + ".whisper.json").absolute())}
        files.append(p['infile'])
        tasklist.append(p)

    hpc = HPCClient(connectuser=args.hpcuser, hpchost=args.hpchost, hpcscript=args.hpcscript,
                    scphost=args.scphost, scpuser=args.scpuser)
    subres = hpc.submit('whisper', {'model': args.model}, tasklist, files)
    print(json.dumps(subres, indent=4))
    jobids = set()
    logging.info(f"These jobs were submitted: {jobids}")

    # wait for the jobs to complete
    logging.info("Waiting for jobs to complete...")
    while jobids:
        fin = set()
        for j in jobids:
            if not hpc.check(j):
                logging.info(f"Job {j} completed")
                fin.add(j)
        jobids -= fin
        if jobids:
            time.sleep(20)

    logging.info("Finished!")


if __name__ == "__main__":
    main()
