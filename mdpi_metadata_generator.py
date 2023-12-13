#!/usr/bin/env hpc_python.sif

import argparse
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from math import floor
from multiprocessing import cpu_count
import json
import logging
from pathlib import Path

import subprocess
import sys
import tempfile

from ffprobe import FFProbe
from performance import Performance
import whisper
import torch
from mediapipe.tasks.python import audio
from mediapipe.tasks.python.components import containers
from scipy.io import wavfile
import mediapipe as mp
import numpy as np


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=sys.path[0] + "/hpc_batch.ini", help="Alternate config file")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument("outdir", type=Path, help="Output directory")
    parser.add_argument("--language", default='en', help="Language to use (or 'auto' to detect)")
    parser.add_argument("--device", choices=['auto', 'cuda', 'cpu'], default='auto', help="Device to use")    
    parser.add_argument("--model", default='large', help="Model to use")
    parser.add_argument("filelist", type=Path, nargs="+", help="File list file for each partition")
    parser.add_argument("--perf", type=Path, help="Performance file")   
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # set up our performance object.
    perf = Performance(args.perf, autosave=True)

    # how many threads should we really start?  ffmpeg tends to use lots of
    # cpus per invocation, so we really don't want to do threads = cores.
    # We want at least as many threads as we have file list partitions plus an
    # additional thread do to the rest of the work.
    ncpu = cpu_count()
    nthreads = min(ncpu, floor(3 + ncpu / 2))
    logging.info(f"Using {nthreads} threads on {ncpu} cpus")

    #  probe all of the files that we will need to process later.  Save the probe
    # data to the disk but keep it in memory too.    
    files: list[tuple[Path, Path, FFProbe]] = []    
    def probe_done_callback(fut: Future):
        r = fut.result()
        files.append(r)
        write_outfile(r[1], args.outdir, "probe", r[2].probe)
        
    ppe = ThreadPoolExecutor(nthreads)
    perf.mark('ffprobes')
    for filelist in args.filelist:
        for file in [Path(x) for x in filelist.read_text().splitlines()]:
            fut = ppe.submit(lambda x: (filelist, x, FFProbe(x)), file)
            fut.add_done_callback(probe_done_callback)    
    ppe.shutdown(wait=True)    
    perf.checkpoint('ffprobes', len(files))

    # fire off all of our processes.
    ppe = ProcessPoolExecutor(nthreads)
    perf.mark("processing")
    def process_done_callback(fut: Future):
        try:
            sperf = fut.result()
            perf.merge(sperf)
        except Exception as e:
            logging.exception(f"Exception in process: {e}")
    
    # there are n*whisper proceseses, one each corresponding to each file list.
    # start them first so they get rolling before the rest of the jobs.
    for fl in args.filelist:        
        fut = ppe.submit(do_whisper, [x for x in files if x[0] == fl], args.outdir,
                         language=args.language, device=args.device)
        fut.add_done_callback(process_done_callback)

    # for the blank_detection and audio_classification tasks, it's one submission
    # per file.    
    for file in files:        
        fut = ppe.submit(do_blankdetection, file[1], file[2], args.outdir, )
        fut.add_done_callback(process_done_callback)
        fut = ppe.submit(do_audioclassification, file[1], file[2], args.outdir)
        fut.add_done_callback(process_done_callback)

    ppe.shutdown(wait=True)
    perf.checkpoint("processing")
    perf.finish()


def write_outfile(srcfile: Path, outdir: Path, key: str, data):
    """Write the output data in json in a reasonable fashion"""
    try:
        with open(outdir / f"{srcfile.name}--{key}.json", "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.exception(f"Cannot write to output file: {srcfile}, {outdir}, {key}, {data}")


def do_whisper(todo: list, outdir: Path, language='en', device='auto', model='large'):    
    perf = Performance(None)    
    if device == 'auto':
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logging.info(f"Using computation device: {device}")

    logging.info(f"Loading {model} model")
    perf.mark("whisper-load-model")
    model_data = whisper.load_model(model, device=device, download_root="/var/lib/whisper")
    perf.checkpoint("whisper-load-model", model, device)

    for audiospec in todo:        
        listfile, audiofile, ffprobe = audiospec        
        afile = str(audiofile.absolute())
        if 'audio' not in ffprobe.get_stream_types():
            logging.info(f"{afile}: Doesn't contain an audio stream.  Skipping")
            continue

        logging.info(f"{afile}: Loading audio file")
        perf.mark("whisper-load-audio")
        audio = whisper.load_audio(afile)
        perf.checkpoint('whisper-load-audio', afile, ffprobe.get_duration())
        
        if language == "auto":            
            logging.info(f"{afile}: Detecting language...")              
            # Just pull the first few languages that are in tokeniser.py
            probable_languages = ('en', 'zh', 'de', 'es', 'ru', 'ko', 'fr', 'ja')
            perf.mark('whisper-detect-language')
            detect_audio = whisper.pad_or_trim(audio)
            mel = whisper.log_mel_spectrogram(detect_audio).to(model.device)
            _, probs = model.detect_language(mel)            
            probs = {k: v for k, v in probs.items() if k in probable_languages}
            logging.info(f"{afile}: Language detection: {probs}")
            language = max(probs, key=probs.get)
            perf.checkpoint('whisper-detect-language', model, language, afile, ffprobe.get_duration())            
            
        logging.info(f"{afile}: Starting {model} transcription")
        perf.mark('whisper-transcribe')
        res = whisper.transcribe(model_data, audio, word_timestamps=True, language=language, verbose=None,
                                 initial_prompt="Hello.")
        perf.checkpoint('whisper-transcribe', model, afile, ffprobe.get_duration())        
        write_outfile(audiofile, outdir, f"whisper-{model}", res)                
        logging.info(f"{afile}: Transcription finished")    
    
    return perf


def do_blankdetection(file: Path, probe: FFProbe, outdir: Path):
    "Run blank detection on a file"
    perf = Performance(None)
    filters = []
    for stype in probe.get_stream_types():
        if stype == 'video':
            filters.append("[0:v]blackdetect=d=5:pix_th=0.10")
        elif stype == 'audio':
            filters.append('[0:a]silencedetect=n=-50dB:d=5')

    afile = str(file.absolute())
    logging.info(f"{afile}: Detecting blank content")
    perf.mark('blankdetect-ffmpeg')
    p = subprocess.run(['ffmpeg', '-i', afile, 
                        '-filter_complex', ";".join(filters),
                        '-f', 'null', '-'],
                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        encoding='utf-8')
    if p.returncode != 0:
        logging.error(f"{afile}: failed to run ffmpeg {p.returncode}: {p.stdout}")
        raise Exception(f"Failed to run ffmpeg for blank detection on {afile}")
    perf.checkpoint('blankdetect-ffmpeg', afile, probe.get_duration())
    
    res = []
    perf.mark('blankdetect-parse')
    linecount = 0
    for line in p.stdout.splitlines():
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


def do_audioclassification(file: Path, probe: FFProbe, outdir: Path):
    "Do audio classification on a file"
    model_path = "/var/lib/mediapipe/yamnet.tflite"
    perf = Performance(None)
    results = []
    afile = str(file.absolute())
    if "audio" not in probe.get_stream_types():
        logging.info(f"{afile}: doesn't contain an audio stream, skipping audioclassification")
        return perf

    with tempfile.TemporaryDirectory() as tmpdir:
        perf.mark("audioclassification-cvt2wav")
        tfile = tmpdir + "/wavefile.wav"
        logging.info(f"{afile}: Converting to wav")                
        p = subprocess.run(['ffmpeg', '-i', afile, tfile],
                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    encoding='utf-8')
        if p.returncode != 0:
            logging.error(f"{afile}: Cannot run ffmpeg for audio classification: {p.stdout}")
            raise Exception(f"FFMPEG failed for audioclassification on {afile}")
        perf.checkpoint("audioclassification-cvt2wav", afile, probe.get_duration())
        
        BaseOptions = mp.tasks.BaseOptions
        AudioRunningMode = mp.tasks.audio.RunningMode
        options = audio.AudioClassifierOptions(
            base_options=BaseOptions(model_asset_path=model_path),
            running_mode=AudioRunningMode.AUDIO_CLIPS,
            max_results=10
        )
        logging.info(f"{afile}: Starting classification")
        perf.mark("audioclassification-classify")
        with audio.AudioClassifier.create_from_options(options) as classifier:
            sample_rate, wav_data = wavfile.read(tfile)
            audio_clip = containers.AudioData.create_from_array(wav_data.astype(float) / np.iinfo(np.int16).max, sample_rate)
            for c in classifier.classify(audio_clip):
                results.append({'timestamp_ms': c.timestamp_ms,
                                'categories': [(y.category_name, y.score) for y in c.classifications[0].categories if y.score >= 0.01]})
        perf.checkpoint("audioclassification-classify", afile, probe.get_duration())
        logging.info(f"{afile}: Classification complete")        
        write_outfile(file, outdir, "audioclassification", results)

    return perf


if __name__ == "__main__":
    main()