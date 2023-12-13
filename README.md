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

