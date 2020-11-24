#!/bin/bash

# we expect the environment variable MPISTAT_HOME to be set so we
# can source the required environemtn
source $MPISTAT_HOME/bin/activate.sh

# run the script to submit the pipeline to the scheduler
python $MPISTAT_HOME/bin/run_mpistat_collect.py $@
