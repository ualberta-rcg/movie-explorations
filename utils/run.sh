#!/bin/bash

set -e

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || exit

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DATA_DIR="${MOVIE_DATA:-$SCRIPT_DIR/..}"

cp $DATA_DIR/movies_json.tar.gz $SLURM_TMPDIR
cd $SLURM_TMPDIR

echo -e "bash $SCRIPT_DIR/make-and-activate-venv.sh
         tar xf movies_json.tar.gz" | parallel

bash $SCRIPT_DIR/launch-notebook.sh
