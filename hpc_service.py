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
from math import floor

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
    
    slurm = Slurm(config['slurm']['account'], config['slurm']['batchdir'])

    if args.command == "submit":
        request = json.loads(sys.stdin.read())
        email = request.get('email', None)
        if email is None: 
            email = config['slurm']['email']

        if request['function'] == 'whisper':
            # load the correct configuration and compute our limits.
            sconfig = config['slurm']            
            params = request['params']            
            wconfig = config[f"{params['engine']}.{params['model']}"]
            if params['device'] == "cuda":
                concurrent_batches = floor(int(sconfig['gpu_vram']) / int(wconfig['model_vram'])) + 1
                processing_factor = float(wconfig['gpu_factor'])
                host_cpus =  4
                host_ram = 64
                gpus = 1
            else:
                concurrent_batches = int(wconfig['cpu_batches'])
                processing_factor = float(wconfig['cpu_factor'])
                host_cpus = concurrent_batches * int(wconfig['cpu_count'])
                host_ram = concurrent_batches * int(wconfig['cpu_model_ram'])                
                gpus = 0

            logging.info(f"Initial resource request:  {host_cpus} cpus, {host_ram} RAM")
            host_ram = min([host_ram, int(sconfig['cpu_ram'])])
            host_cpus = min([host_cpus, int(sconfig['cpu_threads'])])

            target_slot_time = int(sconfig['max_slot_target'])
            max_content_time = target_slot_time * processing_factor
            
            logging.info(f"({params['engine']}.{params['model']}) on {params['device']}, there are {concurrent_batches} concurrent batches each with a max content time of {max_content_time} requiring {host_cpus} CPUS, {host_ram} RAM, and {gpus} GPUS")

            batches = [[]]
            batch_sizes = [0.0]
            for p in request['tasklist']:
                if p['infile'] not in request['probes']:
                    logging.warning(f"Input file {p['infile']} has not been probed.  Skipping")
                    continue
                if 'audio' not in request['probes'][p['infile']]['_stream_types']:
                    logging.warning(f"Input file {p['infile']} doesn't have an audio stream.  Skipping")
                    continue

                d = float(request['probes'][p['infile']]['format']['duration'])
                p['duration'] = d
                if batch_sizes[-1] + d > max_content_time:
                    # only start a new one if there's something already in this batch
                    if len(batches[-1]) > 0:                        
                        batches.append([])
                        batch_sizes.append(0)
                batch_sizes[-1] += d
                batches[-1].append(p)
                
            # group the batches into jobs
            #batch_per_job = int(config['whisper']['concurrent'])
            jobs = [batches[i:i+concurrent_batches] for i in range(0, len(batches), concurrent_batches)]            
            jobids = []
            for j in jobs:
                data = {
                    'scphost': request['scphost'],
                    'scpuser': request['scpuser'],
                    'params': params,
                    'batches': j
                }
                p = sys.path[0].replace("/geode2/", "/N/")
                
                scriptbody = f"time apptainer run --nv {p}/hpc_python.sif {p}/hpc_whisper_server.py <<EOF\n"
                scriptbody += json.dumps(data, indent=4) + "\n"
                scriptbody += "EOF\n"
                job_slot_time = int(target_slot_time * 1.5 / 60)
                host_cpus = min([int(sconfig['cpu_threads']), host_cpus])
                jobids.append(slurm.submit(scriptbody, email, gpu=gpus, cpu=host_cpus, job_time=job_slot_time, ram=host_ram, tag=params['engine']))
        


            print(json.dumps(jobids))


    
    elif args.command == "check":
        print(json.dumps(slurm.get_job_info(args.id, active=True)))
    elif args.command == "list":
        print(json.dumps(slurm.get_job_info(active=True)))
    elif args.command == "cancel":
        print(slurm.cancel_job(args.id))


if __name__ == "__main__":
    main()
