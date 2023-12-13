#!/usr/bin/env hpc_python.sif

import argparse
from concurrent.futures import Future, ProcessPoolExecutor
import json
import logging
from pathlib import Path
import subprocess
import sys
import time

from ffprobe import FFProbe
from performance import Performance
import whisper
import torch

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=sys.path[0] + "/hpc_batch.ini", help="Alternate config file")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument("outdir", type=Path, help="Output directory")
    parser.add_argument("--language", default='en', help="Language to use (or 'auto' to detect)")
    parser.add_argument("--device", choices=['auto', 'cuda', 'cpu'], default='auto', help="Device to use")    
    parser.add_argument("--model", default='large', help="Model to use")
    parser.add_argument("filelist", type=Path, nargs="+", help="File list file for each partition")    
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # We're going to assume (rightly?) that our current directory is batch directory
    # that contains the batch.sh, <jobname>.{out,err}, etc.  This makes things
    # easier since we can store our performance data here without fear of overwriting
    # things!
    batchname = Path('.').absolute().name
    # Create our performance object
    bperf = Performance(f"{batchname}.perf", f"Start processing batch {batchname}")        
    files: list[tuple[Path, Path, FFProbe]] = []    
    # probe all of the files that we will need to process later.  Save the probe
    # data to the disk but keep it in memory too.    
    def probe_done_callback(fut: Future):
        r = fut.result()
        files.append(r)
        (args.outdir / r[1].name + ".probe").write_text(json.dumps(r[2].probe, indent=2))
        
    ppe = ProcessPoolExecutor()
    bperf.checkpoint("Starting FFProbe for all files")
    for filelist in args.filelist:
        for file in [Path[x] for x in filelist.read_text().splitlines()]:
            fut = ppe.submit(lambda x: (filelist, x, FFProbe(x)), file)
            fut.add_done_callback(probe_done_callback)    
    ppe.shutdown(wait=True)    

    # fire off all of our processes.
    ppe = ProcessPoolExecutor()
    # there are n*whisper proceseses, one each corresponding to each file list.
    # start them first so they get rolling before the rest of the jobs.
    for fl in args.filelist:
        sperf = Performance(fl.name + ".whisper.perf", f"Whisper detection for {fl.name} submitted")
        ppe.submit(do_whisper, [x for x in files if x[0] == fl], args.outdir, sperf, 
                   language=args.language, device=args.device)

    # for the blank_detection and audio_classification tasks, it's one submission
    # per file.    
    for file in files:
        sperf = Performance(f.name + ".blankdetection.perf", f"Blank detection submitted for {f.name}")
        ppe.submit(do_blankdetection, f, probes[f], args.outdir, sperf, name=f"{f.name} blank detection")
        sperf = Performance(f.name + ".audioclassification.perf", f"Audio classification submitted for {f.name}")
        ppe.submit(do_audioclassification, f, args.outdir, sperf,  name=f"{f.name} audio classification")

    ppe.shutdown(wait=True)
    bperf.finish()



def do_whisper(todo: list, outdir: Path, perf: Performance, language='en', device='auto', model='large'):    
    perf.checkpoint(f"Whisper starting")
    if device == 'auto':
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logging.info(f"Using computation device: {device}")

    logging.info(f"Loading {model} model")
    mname = model
    model = whisper.load_model(model, device=device, download_root="/var/lib/whisper")
    perf.checkpoint("Loaded Model")

    for audiospec in todo:        
        listfile, audiofile, ffprobe = audiospec
        
        logging.info(f"Loading audio file {audiofile!s}")
        perf.checkstart()
        audio = whisper.load_audio(audiofile)
        perf.checkpoint(f"Audio load time for {audiofile!s}, duration {ffprobe.get_duration()}")

        if language == "auto":            
            logging.info("Detecting language...")  
            # Just pull the first few languages that are in tokeniser.py
            probable_languages = ('en', 'zh', 'de', 'es', 'ru', 'ko', 'fr', 'ja')
            detect_audio = whisper.pad_or_trim(audio)
            mel = whisper.log_mel_spectrogram(detect_audio).to(model.device)
            _, probs = model.detect_language(mel)            
            probs = {k: v for k, v in probs.items() if k in probable_languages}
            logging.info(f"{name}: Language detection: {probs}")
            language = max(probs, key=probs.get)
            logging.info(f"{name}: Detected Language: {language}")        
            prf.checkpoint(f"{name}: detect_language")
        
        logging.info(f"{name}: Starting {mname} transcription for {audiofile!s}")
        res = whisper.transcribe(model, audio, word_timestamps=True, language=language, verbose=None)
        prf.checkpoint("transcription")
        write_outfile(audiofile, outdir, name, res)
        write_outfile(audiofile, outdir, name + "--perf", prf.finish())
        perf.checkpoint(f"File: {audiofile!s}")
        logging.info(f"{name}: Transcription {mname} finished for {audiofile!s}")    


def do_blankdetection(file: Path, probe: FFProbe, outdir: Path, perf: Performance):
    "Run blank detection on a file"


def do_audioclassification(file: Path, outdir: Path, perf: Performance):
    "Do audio classification on a file"






if __name__ == "__main__":
    main()