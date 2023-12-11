# FFMPEG helpers
import subprocess
import json


class FFProbe:
    def __init__(self, filename):
        self.filename = filename
        p = subprocess.run(['ffprobe', '-print_format', 'json', '-show_format', '-show_streams', filename],
                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding='utf-8')
        if p.returncode != 0:
            self.probe = None
        else:            
            self.probe = json.loads(p.stdout)


    def probed_successfully(self):
        """Return true if we got a good probe of the file"""
        return self.probe is not None


    def get_duration(self):
        """Return the duration in seconds"""
        if not self.probe:
            return 0
        
        if 'duration' in self.probe['format']:
            return float(self.probe['format']['duration'])
        elif 'duration' in self.probe['streams'][0]:
            return float(self.probe['streams'][0]['duration'])
        else:
            return 0


    def get_stream_types(self):
        """Return a dict of the type of streams and their counts"""
        if not self.probe:
            return {}
        
        res = {}
        for s in self.probe['streams']:
            if s['codec_type'] not in res:
                res[s['codec_type']] = 0
            res[s['codec_type']] += 1

        return res


