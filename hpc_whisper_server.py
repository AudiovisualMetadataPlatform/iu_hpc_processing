#!/usr/bin/env python3
import sys
import json
import paramiko
import getpass
import whisper
from faster_whisper import WhisperModel
import torch
import logging
from concurrent.futures import Future, ProcessPoolExecutor
import argparse
from pathlib import Path
import subprocess
from utils import write_outfile
import os
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debug")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # determine our queue time based on when the script was created and the current time.
    queue_time = time.time() - Path(__file__).stat().st_mtime
    logging.info(f"Possible queue time: {queue_time} seconds")

    # all of our job parameters come in via a json on stdin.
    data = json.load(sys.stdin)

    # locate the scp keypair
    if data['scpuser'] != getpass.getuser():
        keyfile = Path.home() / f".ssh/{data['scpuser']}.id_rsa"
    else:
        keyfile = Path.home() / ".ssh/id_rsa"

    ppe = ProcessPoolExecutor(len(data['batches']))
    logging.info("Submitting batches")
    for b in data['batches']:
        ppe.submit(do_whisper, b, data['params'], scphost=data['scphost'], scpuser=data['scpuser'], keyfile=keyfile)
    ppe.shutdown(wait=True)
    logging.info("Batches have completed")


def do_whisper(todo: list, params: dict, scphost='localhost', scpuser=None, keyfile=None):   
    device = params['device'] if params['device'] != 'auto' else ('cuda' if torch.cuda_is_available() else 'cpu')    
    logging.info(f"Using {params['model']} on computation device {device} with engine {params['engine']}")
    if params['engine'] == 'whisper':
        model = whisper_load_model(params['model'], device)
    else:
        model = faster_whisper_load_model(params['model'], device)

    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        #key = paramiko.PKey().from_private_key_file(str(keyfile))
        key = paramiko.RSAKey(filename=str(keyfile))
        ssh.connect(scphost, username=scpuser, pkey=key)
        sftp = ssh.open_sftp()
    except Exception as e:
        logging.exception(f"host: {scphost}, user: {scpuser}, keyname: {keyfile}")
        raise e

    logging.info(f"Connected via sftp: {sftp!s}, todo: {todo}")
    pid = os.getpid()
    for spec in todo:
        logging.info(f"Processing {spec}")
        try:
            logging.info(f"Retrieving {spec['infile']}")
            with open(f"media-{pid}.mp4", "wb") as o:
                with sftp.open(spec['infile'], "rb") as i:
                    while len(data := i.read()) > 0:
                        o.write(data)
            
            logging.info(f"Normalizing audio")
            p = subprocess.run(['ffmpeg', '-i', f'media-{pid}.mp4', 
                                '-r', '44100', '-ac', '1', '-c:a', 'pcm_s16le', 
                                f'audio-{pid}.wav'], stdin=subprocess.DEVNULL, 
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                encoding='utf-8')
            if p.returncode != 0:
                raise Exception(f"Cannot run ffmpeg on {spec['infile']}: {p.stdout}")

            t = time.time()
            if params['engine'] == 'whisper':
                results = whisper_impl(pid, spec, model, device, params)
            else:
                results = faster_whisper_impl(pid, spec, model, device, params)
            runtime = time.time() - t
            
            # inject the job parameters and whatnot into the results.
            results['_job'] = {
                'runtime': runtime,
                'media_duration': spec['duration'],
                'job_name': os.environ.get('SLURM_JOB_NAME', 'no job name'),
                'job_id': os.environ.get('SLURM_JOB_ID', 'no job id'),
                'params': params,
                'infile': spec['infile'],
                'outfile': spec['outfile'],
                'scp_callback': f"{scpuser}@{scphost}"
            }


            with open(f"transcript-{pid}.json", "w") as f:
                json.dump(results, f, indent=4)            
            
            logging.info(f"{spec['infile']}: {params['engine']} {params['model']} Transcription finished, {spec['duration']} seconds of content in {runtime} seconds, content ratio {spec['duration'] / runtime}")    
            
            with sftp.open(spec['outfile'], 'w') as o:
                with open(f"transcript-{pid}.json", "r") as i:
                    while len(data := i.read()) > 0:
                        o.write(data)
                        
            logging.info(f"{spec['outfile']} has been transferred back")

        except Exception as e:
            logging.exception(f"Exception during whisper for {spec['infile']}: {e}")

        finally:
            for f in (f'media-{pid}.mp4', f'audio-{pid}.wav', f'transcript-{pid}.json'):
                Path(f).unlink(missing_ok=True)


def whisper_load_model(model, device):
    return whisper.load_model(model, device=device, download_root="/var/lib/whisper")


def whisper_impl(pid, spec, model_data, device, params):
    audio = whisper.load_audio(f"audio-{pid}.wav")
    if params['language'] == "auto":            
        logging.info(f"{spec['infile']}: Detecting language...")              
        # Just pull the first few languages that are in tokeniser.py
        probable_languages = ('en', 'zh', 'de', 'es', 'ru', 'ko', 'fr', 'ja')                
        detect_audio = whisper.pad_or_trim(audio)
        mel = whisper.log_mel_spectrogram(detect_audio).to(device)
        _, probs = model_data.detect_language(mel)            
        probs = {k: v for k, v in probs.items() if k in probable_languages}
        logging.info(f"{spec['infile']}: Language detection: {probs}")
        language = max(probs, key=probs.get)                
    
    logging.info(f"{spec['infile']}: Starting {params['model']} transcription, duration {spec['duration']}")            
    res = whisper.transcribe(model_data, audio, word_timestamps=True, language=params['language'], verbose=None,
                            initial_prompt="Hello.")            
    return res


def faster_whisper_load_model(model, device):
    if device == 'cuda':
        ctype = 'float16'
        threads = 4
    else:
        ctype = 'float32'
        threads = 16
    logging.info(f"Loading faster_whisper model: {model}, {device}, {ctype}, {threads}")
    return WhisperModel(model, device=device, compute_type=ctype, cpu_threads=threads) #, download_dir="/var/lib/faster_whisper")


def faster_whisper_impl(pid, spec, model_data, device, params):    
    segiter, info = model_data.transcribe(f"audio-{pid}.wav", word_timestamps=True, language=params['language'], vad_filter=params['vad'])
    logging.info(f"Using language {info.language}")
    res = {
        'faster_whisper_info': info,
        'language': info.language,
        'text': '',
        'segments': []
    }
    logging.info(f"{spec['infile']}: Starting {params['model']} transcription, duration {spec['duration']}")                
    for s in segiter:
        seg = {
            'id': s.id,
            'seek': s.seek,
            'start': s.start,
            'end': s.end,
            'text': s.text,
            'tokens': s.tokens,
            'temperature': s.temperature,
            'avg_logprob': s.avg_logprob,
            'compression_ratio': s.compression_ratio,
            'no_speech_prob': s.no_speech_prob,
            'words': []
        }        
        res['text'] += s.text
        for w in s.words:
            seg['words'].append({'start': w.start, 'end': w.end, 'word': w.word, 'probability': w.probability})
        res['segments'].append(seg)
    return res


if __name__ == "__main__":
    main()