#!/bin/bash

set -e

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || exit

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DATA_DIR="${MOVIE_DATA:-$SCRIPT_DIR/..}"

cd $SLURM_TMPDIR

echo -e "cp $DATA_DIR/movies_json.tar $SLURM_TMPDIR; tar xf movies_json.tar
bash $SCRIPT_DIR/make-and-activate-venv.sh" | time parallel

bash $SCRIPT_DIR/launch-notebook.sh
