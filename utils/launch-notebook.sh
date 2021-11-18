#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
NOTEBOOK_DIR="$SCRIPT_DIR/../notebooks"

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || exit

source $SCRIPT_DIR/make-and-activate-venv.sh

HOST=$(hostname -f)

echo "Create tunnel with:"
echo -e "
ssh -L 8888:$HOST:8888 -L 8787:$HOST:8787 ${CC_CLUSTER}.computecanada.ca
"
cd $NOTEBOOK_DIR
jupyter notebook --ip $HOST --no-browser
