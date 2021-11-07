#!/bin/bash

set -e

# Assumption:
#   Data is in XML files, with this general pattern:
#     $SLURM_TMPDIR/xml/20191206/can/191206S.XML
#       ...
#     $SLURM_TMPDIR/xml/20201106/use/201106S.XML
#       ...
#     etc.
#
# We will convert to JSON files with structure
#     $SLURM_TMPDIR/json/20191206/can/191206S.json
#       ...
#     $SLURM_TMPDIR/json/20201106/use/201106S.json
#       ...
#     etc.

# Then we will tar up the entire works into a single file at
# $SLURM_TMPDIR/movies_json.tar.gz
# This can then be copied back to permanent storage

[ -n "$SLURM_TMPDIR" ] || echo "No temp dir"
[ -n "$SLURM_TMPDIR" ] || exit

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

source $SCRIPT_DIR/make-and-activate-venv.sh

PYTHON_DIR="$SCRIPT_DIR/../movies_dask_bag"
INPUT_DIR="$SLURM_TMPDIR/xml"
INPUT_DIR_TXT='$SLURM_TMPDIR/xml'

OUTPUT_DIR="$SLURM_TMPDIR/json"
OUTPUT_DIR_TXT='$SLURM_TMPDIR/json'

TASK_FILE="$SLURM_TMPDIR/task-list.txt"
XML_FILES=`ls $INPUT_DIR/*/*/*.XML`

# Empty contents of task file
> $TASK_FILE

# Make list of tasks ...
for input_file in $XML_FILES; do
    input_file_txt=`echo $input_file | \
                    sed "s:$INPUT_DIR:$INPUT_DIR_TXT:"`
    output_file=`echo $input_file | \
                 sed "s:$INPUT_DIR:$OUTPUT_DIR:" | \
                 sed 's:XML$:json:'`
    output_file_txt=`echo $output_file | \
                    sed "s:$OUTPUT_DIR:$OUTPUT_DIR_TXT:"`
    output_dir=`dirname $output_file`
    output_dir_txt=`echo $output_dir | \
                    sed "s:$OUTPUT_DIR:$OUTPUT_DIR_TXT:"`

    echo "echo $output_file_txt; \
          mkdir -p $output_dir_txt; \
          python $PYTHON_DIR/xml_parser.py $input_file_txt -o $output_file_txt" \
          >> $TASK_FILE
done

# Run tasks in parallel

parallel < $TASK_FILE

cd $SLURM_TMPDIR
tar cvzf movies_json.tar.gz json
