# Functions manipulating slurm
import subprocess
import json

class Slurm:
    def __init__(self, account: str, email: str = None):
        self.account = account
        self.email = email
        self.info_cache = None


    def get_job_info(self, jobid=None, jobname=None):
        """Get job information for all jobs with this account, filtered by
           jobid or jobname"""
        p = subprocess.run(['squeue', '-A', self.account, '--json'],
                           stdout=subprocess.PIPE, encoding='utf-8')
        if p != 0:
            return {}
        
        raw = json.loads(p.stdout)
        res = {}
        for j in raw['jobs']:
            if j['account'] == self.account:                
                if jobid is not None and jobid != j['job_id']:
                    continue
                if jobname is not None and jobname != j['name']:
                    continue                
                res[j['job_id']] = j
        return res

    
    def cancel_job(self, jobid: str):
        """Cancel a job"""
        p = subprocess.run(['scancel', '-A', self.account, jobid],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           encoding='utf-8')
        return p.returncode == 0
    

    def submit(self, command):
        """Submit a new batch job, returning the id"""


