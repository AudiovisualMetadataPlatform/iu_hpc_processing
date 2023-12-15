#!/usr/bin/env python3

import argparse
from pathlib import Path
from ffprobe import FFProbe
import logging
from concurrent.futures import ProcessPoolExecutor, Future


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", nargs="+", type=Path, help="file(s) to get duration of")
    
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    ppe = ProcessPoolExecutor()
    total_duration = 0
    durations = []
    def probe_done(fut: Future):
        durations.append(fut.result())


    for f in args.file:
        f = Path(f)
        if f.is_dir():            
            for fx in f.glob('*'):               
                fut = ppe.submit(get_duration, fx)
                fut.add_done_callback(probe_done)               
        else:
            fut = ppe.submit(get_duration, f)
            fut.add_done_callback(probe_done)
    ppe.shutdown(wait=True)
    total_duration = sum(durations)

    seconds = total_duration
    hours = int(seconds / 3600)
    seconds -= hours * 3600
    minutes = int(seconds / 60)
    seconds -= minutes * 60

    print(f"Total duration: {total_duration:0.3f} seconds,  {hours:02d}:{minutes:02d}:{seconds:06.3f}")

def get_duration(f: Path):
    return FFProbe(f).get_duration()


if __name__ == "__main__":
    main()