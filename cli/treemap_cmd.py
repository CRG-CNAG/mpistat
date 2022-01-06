'''
print various reports on space usage using mpistat data
in a clickhouse database
'''

from time import time
import sys
import argparse
from datetime import datetime
import dateparser

import treemap


def get_args():
    '''
    parse commandline arguments
    most of the arguments are related to how we want
    to filter the clickhouse queries
    '''
    parser = argparse.ArgumentParser(description='''
        print various reports on space usage using mpistat data
        in a clickhouse database''')
    parser.add_argument(
        '-p', '--path',
        help='directory we want to report on')
    parser.set_defaults(path='/')
    parser.add_argument(
        '-g', '--group',
        help='group to filter on')
    parser.add_argument(
        '-u', '--user',
        help='user to filter on')
    parser.add_argument(
        '-m', '--modified_before',
        help='select rows where the last modified time is'
             ' earlier than this timestamp (uses dateparser syntax)')
    parser.add_argument(
        '-M', '--modified_after',
        help='select rows where the last modified time is'
             ' later than this timestamp (dateparser syntax)')
    parser.add_argument(
        '-a', '--accessed_before',
        help='select rows where the last accessed time is'
             ' earlier than this timestamp (dateparser syntax)')
    parser.add_argument(
        '-A', '--accessed_after',
        help='select rows where the last accessed time is'
             ' later than this timestamp (dateparser syntax)')
    parser.add_argument(
        '-s', '--size_less_than',
        help='select rows where the file size is less than'
             ' the given size',
        type=int)
    parser.add_argument(
        '-S', '--size_greater_than',
        help='select rows where the file size is greater than the given size',
        type=int)
    parser.add_argument(
        '--suffix',
        help='select rows with the given file suffix (case sensitive)')
    parser.add_argument(
        '--regex',
        help='''
            select rows where the full path matches the given
            regular expression.  must do double-escape on backslash.
            only use with other filters otherwise the query will be
            very slow''')
    parser.add_argument(
        '-d', '--database', nargs='*',
        help='the clickhouse database to use. if you omit this parameter it'
        ' will decide on the appropriate one.'
        ' if doing a diff you can pass up to 2 databases')
    parser.add_argument(
        '-o', '--order_by',
        help='the field to order the table by. should be size, num_files or atime_cost')
    parser.set_defaults(order_by='size')
    parser.add_argument(
        '--by_suffix',
        help='display a suffix report',
        action='store_true')
    parser.set_defaults(by_suffix=False)
    parser.add_argument(
        '--by_user',
        help='display a user report',
        action='store_true')
    parser.set_defaults(by_user=False)
    parser.add_argument(
        '--by_group',
        help='display a group report',
        action='store_true')
    parser.set_defaults(by_group=False)
    parser.add_argument(
        '--list_databases',
        help='show a list of available databases, most recent first',
        action='store_true')
    parser.set_defaults(list_databases=False)
    parser.add_argument(
        '--limit', type=int,
        help='number of rows to show for suffix or user reports')
    parser.set_defaults(limit=20)
    parser.add_argument(
        '--name_width', type=int,
        help='size of name column to ue for tables')
    parser.set_defaults(name_width=21)
    parser.add_argument(
        '-t', '--tag',
        help='choose the latest database with this tag')
    parser.set_defaults(tag=treemap.default_tag())
    parser.add_argument(
        '--diff',
        help='report differences between 2 databases')
    args = parser.parse_args()
    args.path = args.path.rstrip('/')

    # parse date arguments
    if args.modified_before is not None:
        args.modified_before = end_of_day(
            dateparser.parse(args.modified_before).timestamp())
    if args.modified_after is not None:
        args.modified_after = end_of_day(
            dateparser.parse(args.modified_after).timestamp())
    if args.accessed_before is not None:
        args.accessed_before = end_of_day(
            dateparser.parse(args.accessed_before).timestamp())
    if args.accessed_after is not None:
        args.accessed_after = end_of_day(
            dateparser.parse(args.accessed_after).timestamp())
    return args


def end_of_day(epoch):
    '''
    returns the timestamp for the end of the day
    for the passed in timestamp
    '''
    epoch_dt = datetime.fromtimestamp(epoch)
    start_dt = epoch_dt.date()
    return datetime(
        start_dt.year, start_dt.month, start_dt.day, 23, 59, 59).timestamp()

def get_databases(args):
    '''
    what databases do we need to use?
    '''

    # list the databases if that is the request
    if args.list_databases:
        print_databases(args.tag)
        sys.exit(0)
   
    if args.diff is None:
        # just want a single database
        if args.database is None:
            # use the latest default tag
        else:
            # check the database supplied exists
    else:
        # we need 2 databases to diff
        if len(args.database) < 1:
            # use the latest 2 default tag ones
        elif len(args.database) < 2:
            # use the latest default tag db and the one passed in
        else:
            # use the databases passed in


def print_databases(tag):
    '''
    print list of available databases
    '''
    databases = treemap.get_databases(tag)
    print('|-' + '-'*30 + '-|')
    print('| {:30s} |'.format('database'))
    print('|-' + '-'*30 + '-|')
    # print in reverse date order (alphabetical for the last 10 characters)
    for name in sorted(databases, key=lambda item: item[-10:], reverse=True):
        print('| {:30s} |'.format(name))
    print('|-' + '-'*30 + '-|')


def print_header(data):
    '''
    print header showing totals for the given path and filters
    '''
    if data['path']  == '':
        data['path'] = '/'
    print('database  : {}'.format(data['database']))
    print('path      : {}'.format(data['path']))
    print('size      : {}'.format(treemap.get_bytes_str(data['size'])))
    print('num_files : {}'.format(treemap.get_num_str(data['num_files'])))
    print('atime_cost: {}'.format(treemap.get_num_str(data['atime_cost'])))


def get_table_line_format(name_width, order_by):
    pct ='{p:{c}>5}{c}{M}{c}'
    pct += '{q:{c}>5}{c}{M}{c}'
    line_format = u'{L}{c}'
    line_format += '{{n:{{c}}<{}}}{{c}}{{M}}{{c}}'.format(name_width)
    line_format += '{s:{c}>10}{c}{M}{c}'
    if order_by == 'size':
        line_format += pct
    line_format += '{N:{c}>10}{c}{M}{c}'
    if order_by == 'num_files':
        line_format += pct
    if order_by == 'atime_cost':
        line_format += '{a:{c}>10}{c}{M}{c}'
        line_format += '{p:{c}>5}{c}{M}{c}'
        line_format += '{q:{c}>5}{c}{R}'
    else:
        line_format += '{a:{c}>10}{c}{R}'
    return line_format

def print_table(data, col_name, order_by, name_width):
    '''
    print a subdir, user or suffix table
    '''

    # get the line format
    line_format = get_table_line_format(name_width, order_by)

    # print the header
    print()
    print_header(data)
    print()
    print(line_format.format(
        n=col_name, s='size', p='%', q='+%', N='num_files',a='atime_cost',
        L=' ', M= ' ', R=' ', c=' '))
    print(line_format.format(
        n='', s='', p='', q='', N='',a='',
        L=u'\u250F', M=u'\u2533', R=u'\u2513', c=u'\u2501'))

    # print the lines for the table
    q=0
    for child in sorted(data['children'], key=lambda k: k[order_by], reverse=True):
        name = child['name']
        if len(name) > name_width:
            name = name[:name_width-1] + '*'
        p=100.0*child[order_by]/data[order_by]
        q += p
        print(line_format.format(
            n=name,
            s=treemap.get_bytes_str(child['size']),
            N=treemap.get_num_str(child['num_files']),
            a=treemap.get_num_str(child['atime_cost']),
            p='{:5.1f}'.format(p),
            q='{:5.1f}'.format(q),
            L=u'\u2503', M=u'\u2503', R=u'\u2503', c=' '))

    # print the footer
    print(line_format.format(
        n='', s='', N='', a='', p='', q='',
        L=u'\u2517', M=u'\u253B', R=u'\u251B', c=u'\u2501'))
    print()


def main():
    '''
    main entry point
    '''

    # note the start time
    start = time()

    # get commandline args
    args = get_args()

    get_databases(args)

    # get the command line arguments and
    # sanitise them
    args = get_args()
    database = args.database
    path = args.path
    group = args.group
    user = args.user
    modified_before = args.modified_before
    modified_after = args.modified_after
    accessed_before = args.accessed_before
    accessed_after = args.accessed_after
    size_less_than = args.size_less_than
    size_greater_than = args.size_greater_than
    suffix = args.suffix
    regex = args.regex
    order_by = args.order_by
    limit = args.limit
    name_width = args.name_width
    diff = args.diff

    if diff is not None:
        print('doing a diff of {}'.format(diff))

    # placeholder for the query data
    # and the column name to display in the table
    data = []
    col_name = ''

    # get the usage by suffix for the given path and filters
    if args.by_suffix:
        data = treemap.by_suffix(
            database, path, group, user,
            modified_before, modified_after, accessed_before, accessed_after,
            size_less_than, size_greater_than, suffix, regex, order_by, limit)
        col_name = 'suffix'

    # get the usage by user for the given path and filters
    elif args.by_user:
        data = treemap.by_user(
            database, path, group, user,
            modified_before, modified_after, accessed_before, accessed_after,
            size_less_than, size_greater_than, suffix, regex, order_by, limit)
        col_name = 'user'

    # get the usage by group for the given path and filters
    elif args.by_group:
        data = treemap.by_group(
            database, path, group, user,
            modified_before, modified_after, accessed_before, accessed_after,
            size_less_than, size_greater_than, suffix, regex, order_by, limit)
        col_name = 'group'

    # get the usage for the sub directories of the given path
    else:
        data = treemap.get_subdir_data(
            database, path, group, user,
            modified_before, modified_after, accessed_before, accessed_after,
            size_less_than, size_greater_than, suffix, regex)
        col_name = 'subdir'


    # print a table of the data returned
    print_table(data, col_name, order_by, name_width)

    # print the total elapsed time for the query
    print('after {} seconds'.format(time()-start))
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
