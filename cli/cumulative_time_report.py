'''
do a graph of cumulative access time and modified time
from an mpistat clickhouse database
'''

import sys
import argparse
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib as mp
import clickhouse_driver

def get_args():
    ''''
    process commandline arguments for the cumulative space / time graph
    '''
    parser = argparse.ArgumentParser(
        description='generate of cumulative space v atime / mtime days')
    parser.add_argument(
        '--database',
        help='which clickhouse database to use',
        required=True)
    parser.add_argument(
        '--host',
        help='clickhouse host',
        required=True)
    parser.add_argument(
        '--num_days',
        help='number of days back in time to include',
        type=int, required=True)
    return parser.parse_args()


def get_data_set(click, time_var, qry_var):
    '''
    get data to graph for a particular time_var (atime,mtime)
    and data type (size or nfiles)
    '''

    # query base text
    qry = f'''
        select {time_var}_days, {qry_var} 
        from files
        where {time_var}_days >=0
        and {time_var}_days < 5000
        group by {time_var}_days
        order by {time_var}_days
    '''

    # get data from clickhouse
    rows = click.execute(qry)

    # work out cumulative values
    accumulator = 0
    data = []
    for row in rows:
        day = row[0]
        val = row[1] + accumulator
        data.append(val)
        accumulator = val

    # transform to graphable format
    X = np.array(range(len(data))).astype(float)
    X /= 365.0
    Ymax = accumulator
    Y = 100.0*np.array(data)/Ymax
    return (X,Y,Ymax)

def main():
    '''
    main entry point
    '''

    # get args
    args = get_args()

    # get connection to the clickhouse database
    click = clickhouse_driver.Client(
       args.host,
       database=args.database,
       compression=True)

    X_atime_size, Y_atime_size, Ymax_atime_size = get_data_set(
        click,
        'atime',
        'sum(blocks*512)/(1024*1024*1024*1024)')

    X_atime_nfiles, Y_atime_nfiles, Ymax_atime_nfiles = get_data_set(
        click, 
        'atime',
        'count(*)/(1000*1000)')

    X_mtime_size, Y_mtime_size, Ymax_mtime_size = get_data_set(
        click,
        'mtime',
        'sum(blocks*512)/(1024*1024*1024*1024)')

    X_mtime_nfiles, Y_mtime_nfiles, Ymax_mtime_nfiles = get_data_set(
        click,   
        'mtime',
        'count(*)/(1000*1000)')

    # do the plot
    mp.use('agg')
    fig, ax1 = plt.subplots()
    plt.title('Cumulative size / nfiles for time since last access / modification')
    color = 'tab:red'
    ax1.set_xlabel('years since last accessed / modified')
    ax1.set_ylabel(f'cumulative size % ({Ymax_atime_size:.2f} TiB)')
    ax1.step(X_atime_size, Y_atime_size, color='tab:red',
        linewidth=0.5, label='atime_size')
    ax1.step(X_mtime_size, Y_mtime_size, color='tab:blue',
        linewidth=0.5, label='mtime_size')
    ax1.grid(True, which='major', linestyle='-')
    ax1.grid(True, which='minor', linestyle='--')
    ax1.set_xlim(xmin=0)
    ax2 = ax1.twinx()
    ax2.set_ylabel(f'cumulative number of files % ({Ymax_atime_nfiles:.2f} Million)')
    ax2.step(X_atime_nfiles, Y_atime_nfiles, color='tab:green',
        linewidth=0.5, label='atime_nfiles')
    ax2.step(X_mtime_nfiles, Y_mtime_nfiles, color='tab:orange',
        linewidth=0.5, label='mtime_nfiles')
    ax1.set_ylim(ymin=0)
    ax2.set_ylim(ymin=0)
    fig.tight_layout()
    fig.legend(loc='upper left', bbox_to_anchor=(0.6, 0.4))
    plt.savefig('cumulative_access_time_plots_{}.png'.format(args.database))
    plt.close()
    return 0

if __name__ == '__main__':
    sys.exit(main())
