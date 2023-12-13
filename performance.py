import time
import json
import logging


class Performance:
    """
    Create performance statistics for a process.

    The performance data is structed as a dictionary of categories of lists
    of information:

    {
        '_script': [<start_time>, <end_time>, <duration>],
        'cat1': [
            [<start_time>, <end_time>, <duration>, *additional_context],
            [<start_time>, <end_time>, <duration>, *additional_context]
            ....
        ],
        'cat2': [
            [<start_time>, <end_time>, <duration>, *additional_context],
            [<start_time>, <end_time>, <duration>, *additional_context],
            ...
        ],
        ...
    }
    Additional context provides more details for further processing.

    The start time for each category entry is created by calling mark() to
    start a timer for the given category.


    """
    def __init__(self, outfile, title="Performance Object Created", autosave=False):
        "Create a performance structure"
        self.marks = {
            '_script_start': time.time()
        }        
        self.perf = {'_script': [self.marks['_script_start'], None, None, title]}        
        self.outfile = outfile
        self.autosave = autosave
        if self.autosave:
            self.save()


    def mark(self, tag):
        "Create (or replace) a timing mark"
        self.marks[tag] = time.time()


    def checkpoint(self, category, *data, mark=None):
        "Write a category checkpoint"
        if mark is None:
            mtime = self.marks.get(category, self.marks['_script_start'])
        else:
            mtime = self.marks[mark]

        now = time.time()
        if category not in self.perf:
            self.perf[category] = []
        self.perf[category].append([mtime, now, now - mtime, *data])
        if self.autosave:
            self.save()
        return self.perf


    def finish(self):
        "Mark the script as finished."
        now = time.time()
        self.perf['_script'][1] = now
        self.perf['_script'][2] = now - self.perf['_script'][0]
        if self.autosave:
            self.save()
        return self.perf


    def save(self):
        "Save the performance to a file"
        if self.outfile is not None:
            with open(self.outfile, "w") as f:
                json.dump(self.perf, f, indent=2)


    def merge(self, other):
        """Merge another perf into this one.
        This only copies the category checkpoints, not the
        script start, marks, or the filename.
        """        
        for cat in other.perf:
            if cat == '_script':
                continue
            if cat not in self.perf:
                self.perf[cat] = other.perf[cat]
            else:
                self.perf[cat].extend(other.perf[cat])

        if self.autosave:
            self.save()
