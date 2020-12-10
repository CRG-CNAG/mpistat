#!/bin/bash

# which queue
#$ -q *** change me ***

#$ -l s_rt=00:00:30
#$ -l h_rt=00:00:30

# where to send stdout
#$ -o {{ batch_run_dir }}

# where to send stderr
#$ -e {{ batch_run_dir }}

# set environment variables
export MPISTAT_HOME={{ mpistat_home }}
export PYTHON_HOME={{ python_home }}
export GCC_LIBS={{ gcc_libs }}
export PATH=$PYTHON_HOME/bin:$PATH
export LD_LIBRARY_PATH=$PYTHON_HOME/lib:$GCC_LIBS:$LD_LIBRARY_PATH

# start message
echo `date "+%Y-%m-%d %T"` starting create on $HOSTNAME

cd {{ batch_run_dir }}
python {{ mpistat_home }}/bin/mpistat_click_create.py --tag {{ tag }} --date {{ date }}

# finish message
echo `date "+%Y-%m-%d %T"`  finished create on $HOSTNAME

