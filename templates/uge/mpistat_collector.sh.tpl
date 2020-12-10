#!/bin/bash

# which queue
#$ -q *** change me ***

# set long time limit (30 hours)
#$ -l s_rt=30:00:00
#$ -l h_rt=30:00:00

# where to send stdout
#$ -o {{ batch_run_dir }}

# where to send stderr
#$ -e {{ batch_run_dir }}

# which parallel environment to use
#$ -pe ompi {{ num_workers }} 


export MPISTAT_HOME={{ mpistat_home }}

#########################
# location of libcircle #
#########################
export CIRCLE_HOME={{ circle_home }}

#############################
# choose MPI Library to use #
#############################
export MPI_HOME={{ mpi_home }}

#####################
# location of boost #
#####################
export BOOST_HOME={{ boost_home }}

########################
# location of protobuf #
########################
export PROTOBUF_HOME={{ protobuf_home }}

########################
# choose python to use #
########################
export PYTHON_HOME={{ python_home }}

###################
# gcc libs needed #
###################
export GCC_LIBS={{ gcc_libs }}

################################
# set PATH and LD_LIBRARY_PATH #
################################
export PATH=$PYTHON_HOME/bin:$MPI_HOME/bin:$PATH
export LD_LIBRARY_PATH=$PYTHON_HOME/lib:$MPI_HOME/lib:$CIRCLE_HOME/lib:$BOOST_HOME/lib:$PROTOBUF_HOME/lib:$GCC_LIBS:$LD_LIBRARY_PATH

mpirun $MPISTAT_HOME/bin/mpistat $@

