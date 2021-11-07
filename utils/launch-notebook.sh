#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || exit

source $SCRIPT_DIR/make-and-activate-venv.sh

HOST=$(hostname -f)

echo "Create tunnel with:"
echo "ssh -L 8888:$HOST:8888 \
-L 8787:$HOST:8787 \
${CC_CLUSTER}.computecanada.ca"

jupyter notebook --ip $HOST --no-browser
