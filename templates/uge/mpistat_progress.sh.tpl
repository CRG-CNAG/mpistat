#!/bin/bash

# which queue
#$ -q *** change me ***

# set time limit
#$ -l s_rt=00:30:00
#$ -l h_rt=00:30:00

# where to send stdout
#$ -o {{ batch_run_dir }}

# where to send stderr
#$ -e {{ batch_run_dir }}

cd {{ batch_run_dir }}
python {{ mpistat_home }}/bin/mpistat_progress.py $1
