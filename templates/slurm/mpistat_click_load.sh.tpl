#!/bin/bash

##################
# slurm settings #
##################
#SBATCH --output={{ batch_run_dir }}/click_load.sh.%a.o%A
#SBATCH --error={{ batch_run_dir  }}/click_load.sh.%a.e%A
#SBATCH --array=0-{{ num_workers-1 }}%{{ click_num_parallel_loads }}
#SBATCH --time=30
#SBATCH --qos= *** change me ***
#SBACTH --mem=4096

# get the task id in the right format
printf -v j "%0{{ digits }}d" $SLURM_ARRAY_TASK_ID

# start message
echo `date "+%Y-%m-%d %T"` starting load $j on $HOSTNAME

# change to the input directory
cd {{ data_dir }}

# insert the inode data using the clickhouse client command line tool
# uses pattern for looping over tuples
# https://stackoverflow.com/questions/9713104/loop-over-tuples-in-bash
IFS="",
for tuple in f,files,  d,directories,  l,symlinks
    do
        set -- $tuple

        # get the data file
        DATA_FILE="{{ batch_run_dir }}/${j}_${1}.out.gz" 
        echo `date "+%Y-%m-%d %T"` inserting $DATA_FILE

        # insert the inode data using the clickhouse client command line tool
        zcat $DATA_FILE | clickhouse-client --host {{ click_host }}  --query "INSERT INTO {{ database }}.$2 FORMAT Protobuf SETTINGS format_schema='{{ mpistat_home }}/src/mpistat.proto3:MpistatMsg'" --compression 1

    done

# finish message
echo `date "+%Y-%m-%d %T"` finished load $j
