#!/bin/bash

# Assumption:
#   Data is in zip files, with this general pattern:
#     $DATA_DIR/20191206/movies_can.zip
#     $DATA_DIR/20201106/movies_usa.zip
#     etc.
#
# We want to move this data to fast disk (under $SLURM_TMPDIR)
# and decompress each individual zip file into a bunch of XML files.
# The paths would look like
#     $SLURM_TMPDIR/data/20191206/can/191206S.XML
#       ...
#     $SLURM_TMPDIR/data/20201106/use/201106S.XML
#       ...
#     etc.
#
# Then we will tar up the entire works into a single file at
# $SLURM_TMPDIR/movies_xml.tar.gz
# This can then be copied back to permanent storage

# Decompress all of the data on fast disk, and make a tarball

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || exit

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DATA_DIR="${MOVIE_DATA:-$SCRIPT_DIR/../data}"

DEST_DIR="$SLURM_TMPDIR/xml"
VENV="$SLURM_TMPDIR/venv"

TIMESTEPS=`ls $DATA_DIR | grep -P '[0-9]{8}'`

for timestep in $TIMESTEPS; do
  COUNTRIES=`ls $DATA_DIR/$timestep/*.zip | \
             sed "s:$DATA_DIR/$timestep/movies_::" | \
             sed 's:.zip$::'`

  for country in $COUNTRIES; do
      out_dir="$DEST_DIR/$timestep/$country"
      zip="$DATA_DIR/$timestep/movies_$country.zip"
      echo "mkdir -p $out_dir; unzip -d $out_dir $zip;"
  done
done | parallel

echo "Archiving"

cd $SLURM_TMPDIR
tar cf movies_xml.tar xml
