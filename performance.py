import time
import json
import logging


class Performance:
    def __init__(self, outfile, title="Performance Object Created"):
        self.perf = []
        self.total_start = time.time()
        self.section_start = time.time()
        self.outfile = outfile
        self.perf.append([title, 0])

    def checkstart(self):
        self.section_start = time.time()

    def checkpoint(self, title):
        now = time.time()
        self.perf.append([title, now - self.section_start])
        self.section_start = now
        self.save()
        return self.perf

    def finish(self):
        self.perf.append(['total_run_time', time.time() - self.total_start])
        self.save()
        return self.perf

    def save(self):
        with open(self.outfile, "w") as f:
            json.dump(self.perf, f, indent=2)
