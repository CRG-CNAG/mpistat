#!/bin/bash

# which queue
#$ -q  *** change me ***

# array job range and parallelism
#$ -t 1-{{ num_workers }}
#$ -tc {{ click_num_parallel_loads }}

# set  time limit of 30 mins
# it shouldn't take longer than this to load a file
#$ -l s_rt=00:30:00
#$ -l h_rt=00:30:00

# where to send stdout
#$ -o {{ batch_run_dir }}

# where to send stderr
#$ -e {{ batch_run_dir }}

# get the task id in the right format
i=$(expr $SGE_TASK_ID - 1)
printf -v j "%0{{ digits }}d" $i

printf "%(%Y-%m-%d %T)T %s\n" -1 "starting task $j"

# change to the input directory
cd {{ batch_run_dir }}

# insert the inode data using the clickhouse client command line tool
# uses pattern for looping over tuples
# https://stackoverflow.com/questions/9713104/loop-over-tuples-in-bash
IFS="",
for tuple in f,files,  d,directories,  l,symlinks
do
    set -- $tuple

    # get the data file
    DATA_FILE="{{ batch_run_dir }}/${j}_${1}.out.gz"

    printf "%(%Y-%m-%d %T)T %s\n" -1 "inserting $DATA_FILE"

    # insert the inode data using the clickhouse client command line tool
    zcat $DATA_FILE | clickhouse-client --host {{ click_host }}  --query "INSERT INTO {{ database }}.$2 FORMAT Protobuf SETTINGS format_schema='{{ mpistat_home }}/src/mpistat.proto3:MpistatMsg'" --compression 1

done
printf "%(%Y-%m-%d %T)T %s\n" -1 "finished task $j"
