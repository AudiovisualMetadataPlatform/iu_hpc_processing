#!/usr/bin/env hpc_python.sif

import argparse
import ffprobe
import configparser
import logging
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=sys.path[0] + "/hpc_batch.ini", help="Alternate config file")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

    # read the config
    config = configparser.ConfigParser()
    config.read(args.config)








if __name__ == "__main__":
    main()