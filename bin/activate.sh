export MPISTAT_HOME=/absolute/path/to/mpistat
export MPI_HOME=/location/of/mpi
export CIRCLE_HOME=/location/of/libcircle
export PYTHON_HOME=$MPISTAT_HOME/venv
export BOOST_HOME=/location/of/boost
export PROTOBUF_HOME=/location/of/protobuf
export GCC_LIBS=/extra/libraries/for/non/system/gcc
export BINUTILS_HOME=/location/of/binutils
export GCC_HOME=/location/of/gcc
export PATH=$PROTOBUF_HOME/bin:$PYTHON_HOME/bin:$MPI_HOME/bin:$BINUTILS_HOME/bin:$GCC_HOME/bin:$PATH
export LD_LIBRARY_PATH=$GCC_LIBS:$PROTOBUF_HOME/lib:$PYTHON_HOME/lib:$MPI_HOME/lib:$BOOST_HOME/lib:$CIRCLE_HOME/lib:$LD_LIBRARY_PATH

