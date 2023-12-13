#!/usr/bin/env hpc_python.sif

import argparse
from ffprobe import FFProbe
import configparser
import logging
import sys
from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Future

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=sys.path[0] + "/hpc_batch.ini", help="Alternate config file")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument("srcdir", help="Source directory")
    parser.add_argument("outdir", help="Output directory")

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # read the config
    config = configparser.ConfigParser()
    config.read(args.config)

    # gather all of the files and put them into batches of the right duration.
    raw_batches = [{'duration': 0,  'files': []}]    
    batch_limit = int(config['slurm']['max_content_time'])
    
    all_files = []
    for file in Path(args.srcdir).glob("*"):
        if not file.is_file():
            logging.warning(f"Skipping {file!s} because it is not a file")
            continue        
        all_files.append(str(file.absolute()))
        
    def ffprobe_done(fut: Future):
        filename, duration = fut.result()
        raw_batches[-1]['duration'] += duration
        raw_batches[-1]['files'].append(filename)
        if raw_batches[-1]['duration'] > batch_limit:
            logging.info(f"Raw Batch {len(raw_batches)}: {raw_batches[-1]['duration']} seconds of content, {len(raw_batches[-1]['files'])} files.")
            raw_batches.append({'duration': 0, 'files': []})

    ppe = ThreadPoolExecutor()
    for f in all_files:
        fut: Future = ppe.submit(lambda x: (x, FFProbe(x).get_duration()), f)            
        fut.add_done_callback(ffprobe_done)
    ppe.shutdown(wait=True)
    logging.info(f"Raw batching finished, {len(raw_batches)} batches sorted, {len(all_files)} files processed.")


    # create the slurm batches
    batchbase = "mdpi-" + hex(int(time.time()))[-8:]
    batchcount = 0

    whisper_concurrent = int(config['whisper']['concurrent'])
    while len(raw_batches):
        this_batches = []
        while len(this_batches) < whisper_concurrent and raw_batches:
            this_batches.append(raw_batches.pop())
        batch_time = max([x['duration'] for x in this_batches])
        batch_time = int((batch_time * 1.25) / 60) # convert to runtime.
        batch_name = f"{batchbase}-{batchcount}"
        batchcount += 1
        

        # start populating the batch-to-be
        thisdir = sys.path[0]
        buildpath = Path(config['files']['batchdir'], batch_name + ".building").absolute()
        buildpath.mkdir()
        batchpath = Path(config['files']['batchdir'], batch_name).absolute()
        infiles = []
        for i, batch in enumerate(this_batches):
            listfile = batchpath / f"filelist-{i}.txt"
            with open(buildpath / listfile.name, "w") as f:
                f.write("\n".join(batch['files']) + "\n")
            infiles.append(str(listfile.absolute()))
            
        script = [
            f"#!/bin/bash",
            f"#SBATCH -J {batch_name}",
            f"#SBATCH -p gpu",
            f"#SBATCH -A {config['slurm']['account']}",
            f"#SBATCH --gpus-per-node 1",
            f"#SBATCH --mail-type=ALL",
            f"#SBATCH --mail-user={config['slurm']['email']}",
            f"#SBATCH -o {batchpath!s}/stdout.txt",
            f"#SBATCH -e {batchpath!s}/stderr.txt",
            f'#SBATCH -t {batch_time}',
            "",
            f"module load apptainer",
            "",
            f"cd {batchpath!s}",
            f"time apptainer run --nv {thisdir}/hpc_python.sif {thisdir}/mdpi_metadata_generator.py {args.outdir} {' '.join(infiles)}",
            f"echo $? >> returncode.txt"
        ]

        with open(buildpath / "batch.sh", "w") as f:
            f.write("\n".join(script) + "\n")

        (buildpath / "batch.sh").chmod(0o755)
        buildpath.rename(batchpath)
        logging.info(f"Created slurm batch {batch_name}, maximum duration {batch_time}")


if __name__ == "__main__":
    main()