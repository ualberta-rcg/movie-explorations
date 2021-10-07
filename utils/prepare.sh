#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DATA_DIR="${MOVIE_DATA:-$SCRIPT_DIR/../data/20191206}"

COUNTRIES=`ls $DATA_DIR/*.zip | \
           sed "s:$DATA_DIR/movies_::" | \
           sed 's:.zip$::'`

for country in $COUNTRIES; do
    out_dir="$DATA_DIR/$country"
    zip="$DATA_DIR/movies_$country.zip"
    mkdir -p $out_dir
    unzip -d $out_dir $zip;
done
