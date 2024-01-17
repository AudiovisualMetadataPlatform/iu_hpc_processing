#!/usr/bin/env iu

import argparse
from pathlib import Path
from performance import Performance
from ffprobe import FFProbe
import logging
import subprocess
from utils import write_outfile
import mediapipe as mp
from mediapipe.tasks.python import audio
import tempfile
from scipy.io import wavfile
from mediapipe.tasks.python.components import containers
import numpy as np


model = "/home/bdwheele/.mediapipe/yamnet.tflite"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputfile", type=Path, help="Input file")
    parser.add_argument("outputfile", type=Path, help="Output json file")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")
    
    probe = FFProbe(args.inputfile)
    perf = do_classification(args.inputfile, probe, args.outputfile)
    perfdata = perf.finish()
    content_duration = probe.get_duration()
    processing_duration = perfdata['_script'][2]
    ratio = content_duration / processing_duration
    mtype = "video" if 'video' in probe.get_stream_types() else 'audio'
    logging.info(f"{args.inputfile.name}: {content_duration}s of {mtype} content took {processing_duration:0.3f}s, {ratio:0.3f}s of content per clock second.")


def do_classification(file: Path, probe: FFProbe, outdir: Path):
    "Run audio classification on a file"
    perf = Performance(None)
    has_audio = 'audio' in probe.get_stream_types()
    has_video = 'video' in probe.get_stream_types()
    res = [{'type': 'file_metadata',
            'start': 0,
            'end': probe.get_duration(),
            'has_video': has_video,
            'has_audio': has_audio}]

    if has_audio:    
        afile = str(file.absolute())
        with tempfile.TemporaryDirectory() as tempdir:
            # create a wav file
            perf.mark('audioclassification-ffmpeg')
            subprocess.run(["ffmpeg", '-loglevel', 'quiet', '-i', afile, tempdir + "/audio.wav"], check=True)
            perf.checkpoint('audioclassification-ffmpeg', afile, probe.get_duration())
            BaseOptions = mp.tasks.BaseOptions
            AudioRunningMode = mp.tasks.audio.RunningMode
            options = audio.AudioClassifierOptions(
                base_options=BaseOptions(model_asset_path=model),
                running_mode=AudioRunningMode.AUDIO_CLIPS,
                max_results=10)
            perf.mark('audioclassification')        
            with audio.AudioClassifier.create_from_options(options) as classifier:
                sample_rate, wav_data = wavfile.read(tempdir + "/audio.wav")
                audio_clip = containers.AudioData.create_from_array(wav_data.astype(float) / np.iinfo(np.int16).max, sample_rate)
                for c in classifier.classify(audio_clip):
                    res.append({'type': 'audioclassification',
                                'timestamp': c.timestamp_ms / 1000, 
                                'categories': [(y.category_name, y.score * 100) for y in c.classifications[0].categories if y.score >= 0.01]})
            perf.checkpoint('audioclassification', len(res))
        
    write_outfile(file, outdir, 'mp_audioclassification', res)
    return perf


if __name__ == "__main__":
    main()