#!/bin/bash

##################
# slurm settings #
##################
#SBATCH --output={{ batch_run_dir }}/mpistat_progress.sh.o%j
#SBATCH --error={{ batch_run_dir }}/mpistat_progress.sh.e%j
#SBATCH --time=30
#SBATCH --qos= *** change me !!! ***
#SBACTH --mem=4096

#############################################
# environment is set by the pipeline runner #
#############################################

cd {{ batch_run_dir }}
python {{ mpistat_home }}/bin/mpistat_progress.py $1
