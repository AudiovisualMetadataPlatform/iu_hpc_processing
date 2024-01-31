# Cluster access/control
from math import ceil, floor
import socket
import which

class NodeResources:
    "Node resource handler"
    def __init__(self, system_ram=0, system_threads=0, gpu_count=0, gpu_ram=0, max_jobs=0, **kwargs):
        self.system_ram = system_ram
        self.system_threads = system_threads
        self.gpu_count = gpu_count
        self.gpu_ram = gpu_ram
        self.max_jobs = max_jobs

    def get_max_jobs(self, resources):
        "Get the maximum number of jobs for the resources specified"
        
        # if we both have a max_jobs parameter then we use whichever is smaller.
        if self.max_jobs != 0 and resources.max_jobs != 0:
            return min([self.max_jobs, resources.max_jobs])
        
        # divide system resources..
        system_ram = floor(self.system_ram / resources.system_ram)
        system_threads = floor(self.system_threads / resources.system_threads)
                
        # if there's a gpu then we look at gpu resources.
        if resources.gpu_count > 0:
            gpu_count = floor(self.gpu_count / resources.gpu_count)
            gpu_ram = floor(self.gpu_ram / resources.gpu_ram)
            count = min([system_ram, system_threads, gpu_count, gpu_ram])
        else:
            count = min([system_ram, system_threads])
        return count
    

    def get_multiple_resources(self, count=1):
        "Get the resources for multiple instances"
        new_resources = NodeResources(system_ram=self.system_ram * count,
                                      system_threads=self.system_threads * count,
                                      gpu_count=self.gpu_count * count,
                                      gpu_ram=self.gpu_count * count,
                                      max_jobs=count)
        # do a quick sanity check - if any of the new resources are larger
        # than us, then we can't handle that many
        if (new_resources.system_ram > self.system_ram 
            or new_resources.system_threads > self.system_threads 
            or new_resources.gpu_count > self.gpu_count
            or new_resources.gpu_ram > self.gpu_ram
            or new_resources.max_jobs > self.max_jobs):
            raise ValueError("Too many resources")

        return new_resources
    
    

class ClusterManager:
    "Cluster factory"
    def __init__(self, config):
        cluster_types = {'local': Local,
                         'slurm': Slurm}
        self.clusters = {}
        for cname, cdata in config.items():
            try:
                self.clusters[cname] = cluster_types[cdata['type']](cdata)
            except ValueError as e:
                # ignore the cluster if we can't deal with it.
                continue

    def get_clusters(self):
        return self.clusters.keys()
    
    def get_cluster(self, cluster):
        return self.clusters.get(cluster, None)
    


class Cluster:
    pass


class Local(Cluster):
    # jobs running on a local machine.  The partition needs to match
    # the short host name to be valid.
    def __init__(self, config):
        self.batch_dir = config.get("batch_dir", "/tmp")
        this_host = socket.gethostname().split(".")[0]
        self.partitions = {}
        for p, pdata in config['partitions'].items():
            if p == this_host:
                self.partitions[p] = NodeResources(**pdata)
                break
        else:
            raise ValueError("No local partition matching hostname")


class Slurm(Cluster):
    # jobs are submitted to slurm and handled there.  The slurm
    # submission happens on the local host (i.e. this is run on HPC
    # login nodes)
    def __init__(self, config):
        self.account = config.get('account', None)
        self.email = config.get('email', None)
        self.batch_dir = config["batch_dir"]
        self.maximum_time = config.get('maximum_time', 12 * 60)
        self.partitions = {}
        for p, pdata in config['partitions'].items():
            self.partitions[p] = NodeResources(**pdata)



