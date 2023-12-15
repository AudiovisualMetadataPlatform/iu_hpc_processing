from pathlib import Path
import logging
import json


def write_outfile(srcfile: Path, outdir: Path, key: str, data):
    """Write the output data in json in a reasonable fashion"""
    # if outdir is really a directory we'll construct a filename based on the
    # sourcefile.  Outherwise we'll treat the outdir as a filename.
    if outdir.is_dir():
        outfile = outdir / f"{srcfile.name}--{key}.json"
    else:
        outfile = outdir
    
    try:
        with open(outfile, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.exception(f"Cannot write to output file: {srcfile}, {outdir}, {key}, {data}")