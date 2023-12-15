# iu_hpc_processing

A system to manage batch jobs in a semi-automated fashion.

## hpc_python.sif
This is a self-contained apptainer file which is a python 3.11 environment and
includes things like ffmpeg and other tools.

## hpc_batch.ini
Configuration file for all things HPC batchy.

## process_batches
Every minute(?) cron will start process_batches.cron which will exit if 
another instance is running, otherwise it will fire up process_batches.py

process_batches.py will monitor the batch directory for job directories.  The
directories are ignored if:
* it ends with .finished, indicating that the job is out of the system
* it ends with .building, indicating that it is in the process of being built
* it doesn't contain batch.sh, the script which is the actual job to run
* the directory name is a job name in our slurm jobs, indicating it's 
  already running

If the directory isn't skipped, two checks are made.  If a stderr.txt file
exists then the job was processed by slurm at some point and the directory is
renamed to .finished.

If nothing else matches, the directory has never been pushed to slurm so it
will submit the job with a job name matching the directory name.


# MDPI metadata generation
With the MDPI content we need to know the duration of the media so it can be 
batched properly...and we need to know it during the processing.  Unfortunately 
that's an expensive operation and it should be computed prior to making the 
batches.

It's assumed the that the pre-computed ffprobe data (in json format) is
stored next to the media file in question with the additional extension '.probe'

To compute a single ffprobe, one runs:
```
ffprobe -loglevel quiet -print_format json -show_format -show_streams $FILE > $FILE.probe
```

To do it in a multi-processing way one can run:
```
ls *.mp4 | parallel --bar "ffprobe -loglevel quiet -print_format json -show_format -show_streams {} > {}.probe"
```

# Dead space in files
This was really the whole start of this process and then I got sidetracked.  

```
ls *.mp4 | parallel --bar "blankdetection.py {} /tmp/{/}.blankdetection.json
```

Using parallel and sshfs it's possible to spread the load across our servers.

On esquilax (where the source files are):
```
cd /tmp
ln -s /srv/storage/mdpi_research .
```

On each of the worker nodes (unicorn, jackrabbit, xcode-07, capybara):
```
cd /tmp
mkdir mdpi_research
sshfs esquilax.dlib.indiana.edu:/srv/storage/mdpi_research /tmp/mdpi_research
```

For the audio files the dead air is really quick to find but less so for the
video.  Video takes ~6 CPUs each, so let's not overload the servers by dividing
their CPUs by 6 when calling the command on esquilax

```
mkdir -p /home/bdwheele/blankdetection_results/SB-ARCHIVES
find /tmp/mdpi_research/by_type/SB-ARCHIVES/video -type f | parallel --progress  --retries 3 --joblog /tmp/parallel.log -S 12/: -S 4/unicorn  -S 4/jackrabbit -S 12/xcode-07.mdpi.iu.edu -S 12/capybara   "/home/bdwheele/iu_hpc_processing/blankdetection.py {} /home/bdwheele/blankdetection_results/{/}.blankdetection.json"
```

but for audio?  Go nuts!

```
mkdir -p /home/bdwheele/blankdetection_results/SB-ARCHIVES
find /tmp/mdpi_research/by_type/SB-ARCHIVES/audio  -type f | parallel --progress  --retries 3 --joblog /tmp/parallel.log -S 72/: -S 24/unicorn  -S 24/jackrabbit -S 72/xcode-07.mdpi.iu.edu -S 72/capybara   "/home/bdwheele/iu_hpc_processing/blankdetection.py {} /home/bdwheele/blankdetection_results/{/}.blankdetection.json"
```

