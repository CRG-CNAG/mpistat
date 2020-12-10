# takes an mpistat stdout log file as input
# picks out the timestamps and # of objects processed data points from the
# relevant lines
# draws graph of number of objects processes against time
# useful for seeing how the mpistat collection scales with number of workers
#
# lines are like this :
# 1556638658:0:libcircle/token.c:207:Objects processed: 1512468 ...

import sys
import re
import numpy as np
import matplotlib as mp
mp.use('agg')
import matplotlib.pyplot as plt

def main():

    # start plot
    plt.ioff()
    plt.title('Millions of objects processed against time')
    plt.xlabel('Time / hours')
    plt.ylabel('Millions of objects processed')

    # regex to match data lines
    pattern = r'(\d+):\d:libcircle/token.c:207:Objects processed: (\d+) ...$'
    re_mpistat = re.compile(pattern)

    # open data file
    data_file = 'mpistat_collector.sh.o{}'.format(sys.argv[1])
    f = open(data_file)

    # lists to hold vars to plot
    x = []
    y = []

    # loop over lines in  each file
    for line in f:

        # see if line matches data line
        m = re_mpistat.match(line)

        if m:
            # stuff data into the lists to plot
            x.append(float(m.group(1)))
            y.append(float(m.group(2)))

    # rebase times to zero and change to hours
    t0 = x[0]
    X = np.array(x)
    Y = np.array(y)
    X -= t0
    X /= 3600.0  # rebase times to zero and change to hours
    Y /= 1000000.0  # millions of objects processed

    # do the plot
    plt.plot(X, Y)
    axes = plt.gca()
    axes.set_xlim([0,None])
    axes.set_ylim([0,None])
    plt.savefig('mpistat_progress.png')


if __name__ == '__main__':
    main()
