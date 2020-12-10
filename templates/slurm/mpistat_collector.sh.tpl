#!/bin/bash

###############
# slurm setup #
###############
#SBATCH --nodes={{ num_workers }}
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --output={{ batch_run_dir }}/mpistat_collector.sh.o%j
#SBATCH --error={{ batch_run_dir }}/mpistat_collector.sh.e%j
#SBATCH --time=1440
#SBATCH --qos= *** change me !! ***
#SBACTH --mem=4096

####################################
# all necessary environment is set #
# in run_mpistat_collect.sh        #
####################################

mpirun $MPISTAT_HOME/bin/mpistat $@
