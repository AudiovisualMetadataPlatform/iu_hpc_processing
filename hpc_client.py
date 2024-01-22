#!/usr/bin/env python3.9

import getpass
import socket
import paramiko
import ffprobe
import json
import argparse
import logging
import sys
import select


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

        # things from the last remote command run...
        self.stdout = ""
        self.stderr = ""



    def _run_remote(self, command, stdin_data=None):
        (stdin, stdout, stderr) = self.client.exec_command(command)
        if stdin_data:
            logging.debug(f"Writing data to remote command: {stdin_data}")
            stdin.write(stdin_data if isinstance(stdin_data, str) else json.dumps(stdin_data))
            stdin.close()
        logging.debug("Waiting for output")
        self.stdout = "".join(stdout.readlines())
        self.stderr = "".join(stderr.readlines())
        print(self.stderr, file=sys.stderr)   
        

    def submit(self, function: str, params: dict, tasklist: list[dict], files: list[str]):
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
            'tasklist': tasklist,
            'probes': probes,
            'email': self.email,
            'scphost': self.scphost,
            'scpuser': self.scpuser,
        }
        self._run_remote(f"{self.hpcscript} submit", sub)
        if not self.stdout:
            raise Exception(f"Cannot submit.  Stderr: {self.stderr}")        
        return json.loads(self.stdout)        


    def check(self, id):
        self._run_remote(f"{self.hpcscript} check {id}")        
        if not self.stdout:
            raise Exception(f"Cannot check.  Stderr: {self.stderr}")        
        data = json.loads(self.stdout)
        if id not in data:
            return None
        return data[id]['job_state']



    def list(self):
        self._run_remote(f"{self.hpcscript} list")        
        if not self.stdout:
            raise Exception(f"Cannot get list.  Stderr: {self.stderr}")        
        #logging.info(self.stdout)
        return json.loads(self.stdout)
        


    def cancel(self, id):
        self._run_remote(f"{self.hpcscript} cancel {id}")        
        if not self.stdout:
            raise Exception(f"Cannot cancel.  Stderr: {self.stderr}")        
        return "".join(self.stdout)


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
    sp.add_argument("--long", '-l', default=False, action="store_true", help="Long listing")
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
        data = hpc.list()
        # clean up the data
        for f in ('accrue_time', 'admin_comment', 'array_job_id', 'array_task_id',
                  'array_max_tasks', 'array_task_string', 'association_id', 'batch_features',
                  'batch_flag', 'flags', 'burst_buffer', 'burst_buffer_state', 
                  'cluster', 'cluster_features', 'container', 'contiguous', 'core_spec',
                  'thread_spec', 'cores_per_socket', 'billable_tres', 'cpus_per_task',
                  'cpu_frequency_minimum', 'cpu_frequency_maximum', 'cpu_frequency_governor',
                  'cpus_per_tres', 'deadline', 'delay_boot', 'dependency', 'eligible_time',
                  'excluded_nodes', 'features', 'federation_origin', 'federation_siblings_active',
                  'federation_siblings_viable', 'gres_detail', 'last_sched_evaulation',
                  'licenses', 'max_cpus', 'max_nodes', 'mcs_label', 'memory_per_tres',
                  'nice', 'tasks_per_core', 'tasks_per_node', 'tasks_per_socket', 'tasks_per_board',
                  'het_job_id', 'het_job_id_set', 'het_job_offset', 'prefer', 'memory_per_node',
                  'memory_per_cpu', 'minimum_cpus_per_node', 'minimum_tmp_disk_per_node',
                  'preempt_time', 'pre_sus_time', 'profile', 'reboot', 'required_nodes',
                  'resize_time', 'resize_cnt', 'resv_name', 'shared', 'show_flags',
                  'sockets_per_board', 'sockets_per_node', 'suspend_time', 'time_minimum',
                  'threads_per_core', 'tres_bind', 'tres_freq', 'tres_per_job', 'tres_per_node',
                  'tres_per_socket', 'tres_per_task', 'wckey'):
            for d in data:
                data[d].pop(f, None)

        if args.long:
            print(json.dumps(data, indent=4))
        else:
            print(json.dumps(list(data.keys())))

    elif args.command == "cancel":
        print(hpc.cancel(args.id))


if __name__ == "__main__":
    main()