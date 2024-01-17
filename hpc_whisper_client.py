#!/usr/bin/env python3
# run whisper on HPC for some local files.

import argparse
from hpc_service import HPCClient
import logging
from pathlib import Path
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', default=False, action="store_true", help="Turn on debugging")
    parser.add_argument('infile', nargs='+', type=Path, help="Input files")
    parser.add_argument('outdir', type=Path, help="Output directory")
    parser.add_argument('--model', default='medium', choices=['tiny', 'base', 'small', 'medium', 'large'], help="Whisper model")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    if not args.outdir.is_dir():
        logging.error("Output directory must be a directory")
        exit(1)

    files = []
    params = []
    for ifile in args.infile:
        p = {'infile': str(ifile.absolute()),
             'outfile': str((args.outfile / ifile.name).absolute()),
             'model': args.model}
        files.append(p['infile'])
        params.append(p)

    hpc = HPCClient()
    jobids = set(hpc.submit('whisper', params, files))
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
