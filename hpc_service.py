#!/usr/bin/env python3
# client and server bits for the HPC Service
import argparse
import logging
import json
import getpass
import socket
import configparser
from slurm import Slurm
import ffprobe
import sys
from pathlib import Path
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument("--config", type=str, default=sys.path[0] + "/hpc_batch.ini", help="alternate config file")
    subparsers = parser.add_subparsers(help="Command", dest='command')
    sp = subparsers.add_parser('submit', help="Submit a new job")
    sp = subparsers.add_parser('check', help="Check job status")
    sp.add_argument("id", help="Job ID")
    sp = subparsers.add_parser('list', help='List all jobs')
    sp = subparsers.add_parser('cancel', help="Cancel job")
    sp.add_argument("id", help="Job ID")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # read the config
    config = configparser.ConfigParser()
    config.read(args.config)
    
    slurm = Slurm(config['slurm']['account'], config['files']['batchdir'])

    if args.command == "submit":
        request = json.loads(sys.stdin.read())
        email = request.get('email', None)
        if email is None: 
            email = config['slurm']['email']

        if request['function'] == 'whisper':
            # group requests into batches with a maximum duration
            batches = [[]]
            batch_sizes = [0.0]
            size_limit = int(config['slurm']['max_content_time'])
            for p in request['tasklist']:
                if p['infile'] not in request['probes']:
                    logging.warning(f"Input file {p['infile']} has not been probed.  Skipping")
                    continue
                if 'audio' not in request['probes'][p['infile']]['_stream_types']:
                    logging.warning(f"Input file {p['infile']} doesn't have an audio stream.  Skipping")
                    continue

                d = float(request['probes'][p['infile']]['format']['duration'])
                if batch_sizes[-1] + d > size_limit:
                    # start a new one
                    batches.append([])
                    batch_sizes.append(0)
                batch_sizes[-1] += d
                batches[-1].append(p)
                
            # group the batches into jobs
            batch_per_job = int(config['whisper']['concurrent'])
            jobs = [batches[i:i+batch_per_job] for i in range(0, len(batches), batch_per_job)]            
            jobids = []
            for j in jobs:
                data = {
                    'scphost': request['scphost'],
                    'scpuser': request['scpuser'],
                    'params': request['params'],
                    'batches': j
                }
                p = sys.path[0].replace("/geode2/", "/N/")
                
                scriptbody = f"time apptainer run --nv {p}/hpc_python.sif {p}/hpc_whisper_server.py <<EOF\n"
                scriptbody += json.dumps(data, indent=4) + "\n"
                scriptbody += "EOF\n"
                jobids.append(slurm.submit(scriptbody, email, gpu=1, job_time=int(size_limit * 1.5/60)))

            print(json.dumps(jobids))


    
    elif args.command == "check":
        print(json.dumps(slurm.get_job_info(args.id, active=True)))
    elif args.command == "list":
        print(json.dumps(slurm.get_job_info(active=True)))
    elif args.command == "cancel":
        print(slurm.cancel_job(args.id))


if __name__ == "__main__":
    main()
