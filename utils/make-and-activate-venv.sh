#!/bin/bash

VENV="$SLURM_TMPDIR/venv"

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || return 0

[ -z "$VIRTUAL_ENV" ] || echo "venv already activated"
[ -z "$VIRTUAL_ENV" ] || return 0

module load python/3.8 geos

if [ -d "$VENV" ]; then
    echo "venv already exists"
else
    echo "Creating virtual environment"
    virtualenv --no-download $VENV
fi

source $VENV/bin/activate

pip install --no-index xmltodict shapely jupyter dask distributed pandas plotly
