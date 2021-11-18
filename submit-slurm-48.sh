#!/bin/bash

#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --mem-per-cpu=4000M
#SBATCH --time=02:59:00

bash utils/run.sh
