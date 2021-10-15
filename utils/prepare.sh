#!/bin/bash

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || exit

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DATA_DIR="${MOVIE_DATA:-$SCRIPT_DIR/../data/20191206}"

DEST_DIR="$SLURM_TMPDIR/data"
VENV="$SLURM_TMPDIR/venv"

COUNTRIES=`ls $DATA_DIR/*.zip | \
           sed "s:$DATA_DIR/movies_::" | \
           sed 's:.zip$::'`

for country in $COUNTRIES; do
    out_dir="$DEST_DIR/$country"
    zip="$DATA_DIR/movies_$country.zip"
    echo "$zip -> $out_dir"
    mkdir -p $out_dir
    unzip -d $out_dir $zip;
done

echo "Creating virtual environment"
module load python/3.8
virtualenv --no-download $VENV
source $VENV/bin/activate
pip install --no-index xmltodict jupyter dask distributed pandas plotly
deactivate

