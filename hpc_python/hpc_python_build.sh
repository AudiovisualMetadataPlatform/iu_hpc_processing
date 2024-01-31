#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

apptainer build $SCRIPT_DIR/hpc_python.sif $SCRIPT_DIR/hpc_python.recipe
