#!/usr/bin/env python3
import sys
import json
import paramiko
import getpass
import whisper
import torch
import logging
from concurrent.futures import Future, ProcessPoolExecutor
import argparse
from pathlib import Path
import subprocess
from utils import write_outfile
import os

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debug")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")


    # all of our job parameters come in via a json on stdin.
    data = json.load(sys.stdin)

    # determine whisper parameters
    device = data['params'].get('device', 'auto')
    language = data['params'].get('language', 'auto')
    model = data['params'].get('model', 'medium')

    # locate the scp keypair
    if data['scpuser'] != getpass.getuser():
        keyfile = Path.home() / f".ssh/{data['scpuser']}.id_rsa"
    else:
        keyfile = Path.home() / ".ssh/id_rsa"


    ppe = ProcessPoolExecutor(len(data['batches']))
    logging.info("Submitting batches")
    for b in data['batches']:
        ppe.submit(do_whisper, b, language=language, device=device, model=model, scphost=data['scphost'], scpuser=data['scpuser'], keyfile=keyfile)
    ppe.shutdown(wait=True)
    logging.info("Batches have completed")


def do_whisper(todo: list, language='auto', device='auto', model='large', scphost='localhost', scpuser=None, keyfile=None):   
    if device == 'auto':
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logging.info(f"Using computation device: {device}")

    logging.info(f"Loading {model} model")
    model_data = whisper.load_model(model, device=device, download_root="/var/lib/whisper")
    pid = os.getpid()
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

    for spec in todo:
        logging.info(f"Processing {spec}")
        try:
            logging.info(f"Retrieving {spec['infile']}")
            with open(f"media-{pid}.mp4", "wb") as o:
                with sftp.open(spec['infile'], "rb") as i:
                    while True:
                        data = i.read()
                        logging.info(f"Read {len(data)} bytes")
                        if len(data) != 0:
                            o.write(data)
                        else:
                            break
            
            #sftp.get(spec['infile'], "media.mp4")

            logging.info(f"Normalizing audio")
            p = subprocess.run(['ffmpeg', '-i', f'media-{pid}.mp4', 
                                '-r', '44100', '-ac', '1', '-c:a', 'pcm_s16le', 
                                f'audio-{pid}.wav'], stdin=subprocess.DEVNULL, 
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                encoding='utf-8')
            if p.returncode != 0:
                raise Exception(f"Cannot run ffmpeg on {spec['infile']}: {p.stdout}")
                
            audio = whisper.load_audio(f"audio-{pid}.wav")
        
            if language == "auto":            
                logging.info(f"{spec['infile']}: Detecting language...")              
                # Just pull the first few languages that are in tokeniser.py
                probable_languages = ('en', 'zh', 'de', 'es', 'ru', 'ko', 'fr', 'ja')                
                detect_audio = whisper.pad_or_trim(audio)
                mel = whisper.log_mel_spectrogram(detect_audio).to(device)
                _, probs = model_data.detect_language(mel)            
                probs = {k: v for k, v in probs.items() if k in probable_languages}
                logging.info(f"{spec['infile']}: Language detection: {probs}")
                language = max(probs, key=probs.get)                
            
            logging.info(f"{spec['infile']}: Starting {model} transcription")            
            res = whisper.transcribe(model_data, audio, word_timestamps=True, language=language, verbose=None,
                                    initial_prompt="Hello.")            
            with open(f"transcript-{pid}.json", "w") as f:
                json.dump(res, f, indent=4)            
            logging.info(f"{spec['infile']}: Transcription finished")    
    
            #sftp.put("transcript.json", spec['outfile'])
            with sftp.open(spec['outfile'], 'w') as o:
                with open(f"transcript-{pid}.json", "r") as i:
                    while True:
                        data = i.read()
                        logging.info(f"Read {len(data)} bytes")
                        if len(data) != 0:
                            o.write(data)
                        else:
                            break
                        
            logging.info(f"{spec['outfile']} has been transferred back")


        except Exception as e:
            logging.exception(f"Exception during whisper for {spec['infile']}: {e}")

        finally:
            for f in (f'media-{pid}.mp4', f'audio-{pid}.wav', f'transcript-{pid}.json'):
                Path(f).unlink(missing_ok=True)




if __name__ == "__main__":
    main()