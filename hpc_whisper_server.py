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

    ssh = paramiko.SSHClient()
    ssh.connect(scphost, username=scpuser, key_filename=keyfile)
    sftp = ssh.open_sftp()

    for spec in todo:
        try:
            logging.info(f"Retrieving {spec['infile']}")
            sftp.get(spec['infile'], "media.mp4")

            logging.info(f"Normalizing audio")
            p = subprocess.run(['ffmpeg', '-i', 'media.mp4', 
                                '-a:r', '44100', '-a:c', '1', '-c:a', 'pcm_s16le', 
                                'audio.wav'], stdin=subprocess.DEVNULL, 
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                encoding='utf-8')
            if p.returncode != 0:
                raise Exception(f"Cannot run ffmpeg on {spec['infile']}: {p.stdout}")
                
            audio = whisper.load_audio("audio.wav")
        
            if language == "auto":            
                logging.info(f"{spec['infile']}: Detecting language...")              
                # Just pull the first few languages that are in tokeniser.py
                probable_languages = ('en', 'zh', 'de', 'es', 'ru', 'ko', 'fr', 'ja')                
                detect_audio = whisper.pad_or_trim(audio)
                mel = whisper.log_mel_spectrogram(detect_audio).to(model.device)
                _, probs = model.detect_language(mel)            
                probs = {k: v for k, v in probs.items() if k in probable_languages}
                logging.info(f"{spec['infile']}: Language detection: {probs}")
                language = max(probs, key=probs.get)                
            
            logging.info(f"{spec['infile']}: Starting {model} transcription")            
            res = whisper.transcribe(model_data, audio, word_timestamps=True, language=language, verbose=None,
                                    initial_prompt="Hello.")            
            
            write_outfile(None, "transcript.json", None, res)                
            logging.info(f"{spec['infile']}: Transcription finished")    
    
            sftp.put("transcript.json", spec['outfile'])
            logging.info(f"{spec['outfile']} has been transferred back")


        except Exception as e:
            logging.exception(f"Exception during whisper for {spec['infile']}: {e}")

        finally:
            for f in ('media.mp4', 'audio.wav', 'transcript.json'):
                Path(f).unlink(missing_ok=True)




if __name__ == "__main__":
    main()