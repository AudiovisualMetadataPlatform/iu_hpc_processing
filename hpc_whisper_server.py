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
    language = data['params'].get('language', 'en')
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


def do_whisper(todo: list, language='en', device='auto', model='large', scphost=localhost, scpuser=None, keyfile=None):   
    if device == 'auto':
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logging.info(f"Using computation device: {device}")

    logging.info(f"Loading {model} model")
    #perf.mark("whisper-load-model")
    model_data = whisper.load_model(model, device=device, download_root="/var/lib/whisper")
    #perf.checkpoint("whisper-load-model", model, device)

    


    for spec in todo:




        listfile, audiofile, ffprobe = audiospec        
        afile = str(audiofile.absolute())
        if 'audio' not in ffprobe.get_stream_types():
            logging.info(f"{afile}: Doesn't contain an audio stream.  Skipping")
            continue

        logging.info(f"{afile}: Loading audio file")
        #perf.mark("whisper-load-audio")
        audio = whisper.load_audio(afile)
        #perf.checkpoint('whisper-load-audio', afile, ffprobe.get_duration())
        
        if language == "auto":            
            logging.info(f"{afile}: Detecting language...")              
            # Just pull the first few languages that are in tokeniser.py
            probable_languages = ('en', 'zh', 'de', 'es', 'ru', 'ko', 'fr', 'ja')
            #perf.mark('whisper-detect-language')
            detect_audio = whisper.pad_or_trim(audio)
            mel = whisper.log_mel_spectrogram(detect_audio).to(model.device)
            _, probs = model.detect_language(mel)            
            probs = {k: v for k, v in probs.items() if k in probable_languages}
            logging.info(f"{afile}: Language detection: {probs}")
            language = max(probs, key=probs.get)
            #perf.checkpoint('whisper-detect-language', model, language, afile, ffprobe.get_duration())            
            
        logging.info(f"{afile}: Starting {model} transcription")
        #perf.mark('whisper-transcribe')
        res = whisper.transcribe(model_data, audio, word_timestamps=True, language=language, verbose=None,
                                 initial_prompt="Hello.")
        #perf.checkpoint('whisper-transcribe', model, afile, ffprobe.get_duration())        
        write_outfile(audiofile, outdir, f"whisper-{model}", res)                
        logging.info(f"{afile}: Transcription finished")    
    
    return perf





if __name__ == "__main__":
    main()