#!/bin/bash

###############
# slurm setup #
###############
#SBATCH --output={{ batch_run_dir }}/mpistat_click_create.sh.o%j
#SBATCH --error={{ batch_run_dir }}/mpistat_click_create.sh.e%j
#SBATCH --time=60
#SBATCH --qos= *** change me!!! ***

#############################################
# environment is set by the pipeline runner #
#############################################

# start message
echo `date "+%Y-%m-%d %T"` starting create on $HOSTNAME

cd {{ batch_run_dir }}
python {{ mpistat_home }}/bin/mpistat_click_create.py --tag {{ tag }} --date {{ date }}

# finish message
echo `date "+%Y-%m-%d %T"` finished create $j
