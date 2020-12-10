'''
create a clickhouse database for an mpistat dataset
uses a jinja2 template to create the schema
need to pass in the database tag
'''

import os
import sys
import argparse
import subprocess
import math
import time

import jinja2
import clickhouse_driver

import mpistat_config


def get_args():
    ''''
    parse the arguments  to create a clickhouse database
    '''
    parser = argparse.ArgumentParser(description='''
        create a clickhouse database for an mpistat dataset.''')
    parser.add_argument(
        '-t', '--tag', required=True,
        help='tag name of the database to create')
    parser.add_argument(
        '--date', required=True,
        help='date the mpistat scan was started')
    return parser.parse_args()


def create_schema(database, jinja_env):
    '''
    create the text for the schema from the
    jinja2 template
    uses the clickhouse-client commandline with --multiquery
    since it's not possible to do that with python
    '''

    # generate the schema string
    context = {
        'database': database,
        'now': int(time.time())
    }
    template = jinja_env.get_template('schema.sql.tpl')
    schema = template.render(context).encode('utf-8')

    # set up the clickhouse=client command
    cmd = ['clickhouse-client', '--host', mpistat_config.click_host, '--multiquery']
    clickhouse_client = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    clickhouse_client.communicate(schema)


def main():
    '''
    main entry point
    '''

    # parse command line arguments
    args = get_args()

    # determine the database name
    database = args.tag + '_' + args.date

    # setup jinja2 templating
    template_path = os.environ['MPISTAT_HOME'] + '/clickhouse'
    jinja_loader = jinja2.FileSystemLoader(searchpath=template_path)
    jinja_env = jinja2.Environment(loader=jinja_loader)

    # create the db schema
    print('creating database schema for {}'.format(database))
    create_schema(database, jinja_env)

    return 0


if __name__ == '__main__':
    sys.exit(main())
