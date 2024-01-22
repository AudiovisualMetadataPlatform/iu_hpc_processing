# Functions manipulating slurm
import subprocess
import json
from pathlib import Path
import time
import os
import logging

class Slurm:
    def __init__(self, account: str, batchdir: str):
        self.account = account
        self.batchdir = Path(batchdir)
        self.batchdir.mkdir(exist_ok=True, parents=True)


    def submit(self, scriptbody, email, gpu=0, job_time="1:00"):
        """Submit a new batch job, returning the id"""
        # create a batch name, directory, and move there.
        job_name = f"job-{time.time()}"
        job_dir: Path = self.batchdir / job_name
        job_dir.mkdir()
        here = Path.cwd()
        os.chdir(job_dir)

        # build the job script
        script = [
            f"#!/bin/bash",
            f"#SBATCH -J {job_name}",
            f"#SBATCH -A {self.account}",
            f"#SBATCH -o {str(job_dir.absolute())}/stdout.txt",
            f"#SBATCH -e {str(job_dir.absolute())}/stderr.txt",
            f'#SBATCH -t {job_time}',
            f"#SBATCH --mail-type=ALL",
            f"#SBATCH --mail-user={email}",
            f"#SBATCH --mem 128G",
        ]

        # if a GPU is requested, add the GPU parameters.
        if gpu > 0:
            script.extend([
                f"#SBATCH -p gpu",
                f"#SBATCH --gpus-per-node {gpu}",
            ])

        # load the usual modules.
        script.extend([
            f"module load apptainer",
            f"module load ffmpeg",
            f"module load python"
        ])

        # make sure we're in the right directory
        script.extend([
            f"cd {job_dir!s}"
        ])

        # add the supplied script body
        script.append(scriptbody)

        # capture the return code
        script.extend([
            "echo $? >> returncode.txt"
        ])

        # write the script
        with open(job_dir / "script.sh", "w") as f:
            f.write("\n".join(script) + "\n")
        (job_dir / "script.sh").chmod(0o755)

        # submit the job to slurm
        if True:
            p = subprocess.run(['sbatch', str((job_dir / "script.sh").absolute())],
                                stdout=subprocess.PIPE, encoding='utf-8')
            output = p.stdout.strip()
            #logging.info(output)
            jobid = int(p.stdout.strip().split()[-1])
            with open(job_dir / "slurm_job.txt", "w") as f:
                f.write(f"{jobid}\n")
            return jobid
        else:
            return job_dir.name


    def get_job_info(self, jobid=None, jobname=None, active=True):
        """Get job information for all jobs with this account, filtered by
           jobid or jobname"""
        p = subprocess.run(['squeue', '-A', self.account, '--json'],
                           stdout=subprocess.PIPE, encoding='utf-8')        
        if p.returncode != 0:
            return {}
        
        raw = json.loads(p.stdout)
        res = {}
        for j in raw['jobs']:
            if j['account'] == self.account:
                if j['job_state'] == 'COMPLETED' and active:
                    continue              
                if jobid is not None and int(jobid) != j['job_id']:
                    continue
                if jobname is not None and jobname != j['name']:
                    continue                
                res[j['job_id']] = j
        return res


    def get_job_details(self, jobid):
        """Get job details"""
        data = self.get_job_info(jobid)
        if jobid in data:
            pass

        p = subprocess.run(['squeue', '-A', self.account, '--json'],
                           stdout=subprocess.PIPE, encoding='utf-8')        
        if p.returncode != 0:
            return {}
        
        raw = json.loads(p.stdout)
        res = {}
        for j in raw['jobs']:
            if j['account'] == self.account:
                if j['job_state'] == 'COMPLETED' and active:
                    continue              
                if jobid is not None and int(jobid) != j['job_id']:
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
    


        

