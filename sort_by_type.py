#!/usr/bin/env python3

import argparse
from pathlib import Path
from ffprobe import FFProbe
import logging
from typing import cast
from concurrent.futures import ProcessPoolExecutor
import os.path

def main():
    parser = argparse.ArgumentParser()
    #parser.add_argument("basedir", type=Path, help="All of the other directories must be relative to this one")
    parser.add_argument("srcdir", type=Path, help="Source directory to scan")
    parser.add_argument("audiodir", type=Path, help="Link directory for audio files")
    parser.add_argument("videodir", type=Path, help="Link directory for video files")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # make sure everything is a directory...
    for d in (args.srcdir, args.audiodir, args.videodir):
        if not d.is_dir():
            logging.error(f"{d} is not a directory")
            exit(1)

    ppe = ProcessPoolExecutor()
    for f in cast(Path, args.srcdir).glob("*"):
        ppe.submit(do_symlink, f, args.videodir, args.audiodir)
    ppe.shutdown(wait=True)


def do_symlink(f: Path, vdir: Path, adir: Path):
    try:
        probe = FFProbe(f)
        dest: Path = vdir if 'video' in probe.get_stream_types() else adir
        dest = dest / f.name
        relf = Path(os.path.relpath(f.parent, dest.parent), f.name)
        dest.symlink_to(relf)
        logging.info(f"Linking {dest} to {relf}")
    except Exception as e:
        logging.exception(e)




if __name__ == "__main__":
    main()