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
    
    slurm = Slurm(config['slurm']['account'])

    if args.command == "submit":
        request = json.loads(sys.stdin)

        if request['function'] == 'whisper':
            batches = []
            cur_size = 0
            for p in request['params']:
                if p['infile'] not in request['probes']:
                    logging.warning(f"Input file {p['infile']} has not been probed.  Skipping")
                    continue

                



    
    elif args.command == "check":
        print(json.dumps(slurm.get_job_info(args.id, active=True)))
    elif args.command == "list":
        print(json.dumps(slurm.get_job_info(active=True)))
    elif args.command == "cancel":
        print(slurm.cancel_job(args.id))


if __name__ == "__main__":
    main()
