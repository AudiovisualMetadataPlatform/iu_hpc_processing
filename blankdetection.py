#!/usr/bin/env python3

import argparse
from pathlib import Path
from performance import Performance
from ffprobe import FFProbe
import logging
import subprocess
from utils import write_outfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputfile", type=Path, help="Input file")
    parser.add_argument("outputfile", type=Path, help="Output json file")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")
    
    probe = FFProbe(args.inputfile)
    perf = do_blankdetection(args.inputfile, probe, args.outputfile)
    perfdata = perf.finish()
    content_duration = probe.get_duration()
    processing_duration = perfdata['_script'][2]
    ratio = content_duration / processing_duration
    mtype = "video" if 'video' in probe.get_stream_types() else 'audio'
    logging.info(f"{args.inputfile.name}: {content_duration}s of {mtype} content took {processing_duration:0.3f}s, {ratio:0.3f}s of content per clock second.")


def do_blankdetection(file: Path, probe: FFProbe, outdir: Path):
    "Run blank detection on a file"
    perf = Performance(None)
    filters = []
    has_audio = False
    has_video = False
    for stype in probe.get_stream_types():
        if stype == 'video':
            filters.append("[0:v]blackdetect=d=60:pix_th=0.10")
            has_video = True
        elif stype == 'audio':
            filters.append('[0:a]silencedetect=n=-60dB:d=60')
            has_audio = True
    #print(filters)

    afile = str(file.absolute())
    logging.info(f"{afile}: Detecting blank content")
    perf.mark('blankdetect-ffmpeg')
    p = subprocess.run(['ffmpeg', '-i', afile, 
                        '-filter_complex', ";".join(filters),
                        '-f', 'null', '-'],
                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout = p.stdout.decode('utf-8', 'replace')
    
    if p.returncode != 0:
        logging.error(f"{afile}: failed to run ffmpeg {p.returncode}: {stdout}")
        raise Exception(f"Failed to run ffmpeg for blank detection on {afile}")
    perf.checkpoint('blankdetect-ffmpeg', afile, probe.get_duration())
    
    res = [{'type': 'file_metadata',
            'start': 0,
            'end': probe.get_duration(),
            'has_video': has_video,
            'has_audio': has_audio}]
    
    perf.mark('blankdetect-parse')
    linecount = 0
    for line in stdout.splitlines():
        linecount += 1
        if 'silence_end' in line:            
            parts = line.split()
            duration = float(parts[7])
            end = float(parts[4])
            start = end - duration
            res.append({'type': 'silence',
                        'start': start,
                        'end': end })
        elif 'black_start' in line:
            parts = line.split()            
            start = float(parts[3].split(':')[1])
            end = float(parts[4].split(':')[1])
            res.append({'type': 'black',
                        'start': start,
                        'end': end})        
    perf.checkpoint('blankdetect-parse', len(res), linecount)
    write_outfile(file, outdir, 'blankdetect', res)
    return perf

if __name__ == "__main__":
    main()