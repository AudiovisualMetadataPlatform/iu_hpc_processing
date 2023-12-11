#!/usr/bin/env mdpi_python.sif

import argparse
import configparser
import logging
from pathlib import Path
import sys

from slurm import Slurm
from ffprobe import FFProbe

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=sys.path[0] + "/hpc_batch.ini", help="Alternate config file")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # read the config
    config = configparser.ConfigParser()
    config.read(args.config)

    # determine if we need to create new jobs
    slurm = Slurm(config['slurm']['account'], config['slurm']['email'])
    job_info = slurm.get_job_info()    

    needed_jobs = config['slurm']['max_concurrent'] - len(job_info)
    if needed_jobs < 1:
        logging.debug("No jobs need to be created")
        exit(0)

    # since we need new jobs, let's look at what still needs to be done.
    # get job name -> job, since the job name is also the batch directory name.
    job_names = {}
    for j in job_info.values():
        job_names[j['name']] = j


    for dir in Path(config['files']['batchdir']).glob("*"):
        if dir.name.endswith(".finished"):
            # this one is finished.
            continue
        if dir.name in job_names:
            # this one is already in slurm, skip it.
            continue

        if (dir / "job.err").exists():
            # this job has an stderr capture file which means that
            # it ran at least a little bit under slurm.  Since it's
            # not in slurm right now, we can go ahead and mark the
            # job as finished by renaming the directory
            logging.info(f"Marking batch {dir.name} as finished")
            dir.rename(dir.with_name(dir.name + ".finished"))
            continue

        if not (dir / "batch.sh").exists():
            # no batch.sh script, so skip it
            continue

        jobid = slurm.submit(str(dir / "batch.sh"))
        logging.info(f"Submitted new job for batch {dir.name} as job id {jobid} ")

        needed_jobs -= 1
        if needed_jobs < 1:
            break



if __name__ == "__main__":
    main()
