#!/usr/bin/env python3.9

import getpass
import socket
import paramiko
import ffprobe
import json
import argparse
import logging


class HPCClient:
    def __init__(self, connectuser=None, hpchost="bigred200.uits.iu.edu", 
                 scpuser=None, scphost=None, email=None,
                 hpcscript="iu_hpc_processing/hpc_service.py"):
        self.connectuser = getpass.getuser() if not connectuser else connectuser
        self.email = email
        self.scpuser = self.connectuser if not scpuser else scpuser
        self.scphost = socket.getfqdn() if not scphost else scphost
        self.hpchost = hpchost
        self.hpcscript = hpcscript
        # set up base ssh client
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.client.connect(self.hpchost, username=self.connectuser)


    def submit(self, function: str, params: list[dict], files: list[str]):
        """Build a submission data packet and send it to HPC for later work.  Return the job ids"""
        # run ffprobe on all of the files.
        probes = {}
        for f in files:
            p = ffprobe.FFProbe(f)
            if p.probed_successfully():
                probes[f] = p.probe

        sub = {
            'function': function,
            'params': params,
            'probes': probes,
            'email': self.email,
            'scphost': self.scphost,
            'scpuser': self.scpuser,
        }

        (stdin, stdout, stderr) = self.client.exec_command(f"{self.hpcscript} submit")
        stdin.write(json.dumps(sub))
        stdin.close()
        data = stdout.readlines()
        if not data:
            raise Exception(f"Cannot submit.  Stderr: {stderr.readlines()}")        
        return json.loads("\n".join(data))        


    def check(self, id):
        (stdin, stdout, stderr) = self.client.exec_command(f"{self.hpcscript} check {id}")
        data = stdout.readlines()
        if not data:
            raise Exception(f"Cannot check.  Stderr: {stderr.readlines()}")        
        return json.loads("\n".join(data))


    def list(self):
        (stdin, stdout, stderr) = self.client.exec_command(f"{self.hpcscript} list")
        data = stdout.readlines()
        if not data:
            raise Exception(f"Cannot get list.  Stderr: {stderr.readlines()}")        
        return json.loads("\n".join(data))
    

    def cancel(self, id):
        (stdin, stdout, stderr) = self.client.exec_command(f"{self.hpcscript} cancel {id}")
        data = stdout.readlines()
        if not data:
            raise Exception(f"Cannot cancel.  Stderr: {stderr.readlines()}")        
        return "".join(data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument("--hpcuser", type=str, default=None, help="User on HPC")
    parser.add_argument("--hpchost", type=str, default="bigred200.uits.iu.edu", help="HPC Host")
    parser.add_argument("--hpcscript", type=str, default="iu_hpc_processing/hpc_service.py")
    subparsers = parser.add_subparsers(help="Command", dest='command', required=True)
    sp = subparsers.add_parser('submit', help="Submit a new job")
    sp = subparsers.add_parser('check', help="Check job status")
    sp.add_argument("id", help="Job ID")
    sp = subparsers.add_parser('list', help='List all jobs')
    sp = subparsers.add_parser('cancel', help="Cancel job")
    sp.add_argument("id", help="Job ID")    
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    hpc = HPCClient(connectuser=args.hpcuser, hpchost=args.hpchost, hpcscript=args.hpcscript)
    if args.command == "submit":
        logging.warning("Use a specific hpc client command to submit a job")
        exit(1)
    elif args.command == "check":
        print(hpc.check(args.id))
    elif args.command == "list":
        print(hpc.list())
    elif args.command == "cancel":
        print(hpc.cancel(args.id))


if __name__ == "__main__":
    main()