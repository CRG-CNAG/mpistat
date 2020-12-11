'''
run the mpistat collector pipeline
'''

import sys
import os
import errno
from datetime import datetime
import argparse
import logging
import math

import jinja2

import mpistat_utils
import mpistat_config


def get_args():
    ''''
    process commandline arguments for the collect pipeline
    '''
    parser = argparse.ArgumentParser(
        description='run the collect  pipeline')
    parser.add_argument(
        '--tag',
        help='tag for the dataset e.g. scratch, home, isilon etc',
        required=True)
    parser.add_argument(
        '--num_workers',
        help='total number of workers',
        type=int, required=True)
    parser.add_argument(
        'seeds',
        nargs='+',
        help='one or more directories to start from')
    return parser.parse_args()



def submit_batch_job(job_args, job_dependency=None):
    '''
    job_args contains the script to run
    and any command line arguments to pass to it
    all paths should be full paths not relative paths
    job_dependency is a job_id this job depends on
    returns the queued job id
    '''
    if mpistat_config.scheduler == 'uge':
        cmd = [
            'qsub',
            '-terse']
    else:
        cmd = [
            'sbatch',
            '--parsable'] 

    # add a job dependency if needed
    if job_dependency is not None:
        if mpistat_config.scheduler == 'uge':
            cmd.append('-hold_jid')
            cmd.append(job_dependency)
        else:
            cmd.append('--dependency=afterok:{}'.format(job_dependency)) 

    # finish constructing the qsub command
    cmd += job_args 
    
    # run the command and capture the output
    print('submitting job...') 
    print(' '.join(cmd))
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode == 0:
        m = re.search(r'^(\d+)', result.stdout.decode('latin1'))
        if m:
            job_id = m.group(1)
            print('got job id {}'.format(job_id))
            return job_id
        else:
            raise Exception(
                    'failed to parse job_id : {}'.format(result.stdout))
    else: print('submission failed...')
    err = result.stderr
    print(err)
    raise Exception(err)
                                                                                                                                                                                                                                                                                                                     
def make_job(jinja_env, batch_run_dir, job_name,
             job_args, job_context, job_dependency=None):
    '''
    create a batch script from a template
    and submit it with a job dependency if required
    '''
    batch_script_path = batch_run_dir + '/' + job_name + '.sh'
    template = jinja_env.get_template(job_name + '.sh.tpl')
    with open(batch_script_path, 'w') as tpl:
        tpl.write(template.render(job_context))

    # create the progress job command
    cmd = [batch_script_path] + job_args

    # submit the progress job with a dependency on the collector job
    return mpistat_utils.submit_batch_job(cmd, job_dependency)


def main():
    '''
    main entry point of program
    '''

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s:%(lineno)d] %(message)s')

    logging.info('starting with args : {}'.format(sys.argv))
    logging.debug('environment is : {}'.format(os.environ))

    # get environment variables
    mpi_home = os.environ['MPI_HOME']
    circle_home = os.environ['CIRCLE_HOME']
    python_home = os.environ['PYTHON_HOME']
    boost_home = os.environ['BOOST_HOME']
    protobuf_home = os.environ['PROTOBUF_HOME']
    mpistat_home = os.environ['MPISTAT_HOME']
    gcc_libs = os.environ['GCC_LIBS']

    # process commandline args
    args = get_args()

    # get the date string
    date_str = datetime.today().strftime('%Y_%m_%d')

    # make the date directory in the data directory
    batch_run_dir = mpistat_home + '/data/'+date_str + '_' + args.tag
    try:
        os.mkdir(batch_run_dir)
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise

    # setup jinja2 templating
    template_path = mpistat_home+"/templates/" + mpistat_config.scheduler
    jinja_loader = jinja2.FileSystemLoader(searchpath=template_path)
    jinja_env = jinja2.Environment(loader=jinja_loader)

    # set up the template context
    digits = int(math.log10(args.num_workers-1) + 1)
    context = {
        'date': date_str,
        'mpi_home': mpi_home,
        'circle_home': circle_home,
        'python_home': python_home,
        'boost_home': boost_home,
        'protobuf_home': protobuf_home,
        'mpistat_home': mpistat_home,
        'gcc_libs': gcc_libs,
        'batch_run_dir': batch_run_dir,
        'num_workers': args.num_workers,
        'digits': digits,
        'tag': args.tag,
        'click_host': mpistat_config.click_host,
        'click_num_parallel_loads': mpistat_config.click_num_parallel_loads,
        'database': args.tag + '_' + date_str
    }

    #####################
    # collector mpi job #
    #####################
    collector_job_id = make_job(
        jinja_env,
        batch_run_dir,
        'mpistat_collector',
        [batch_run_dir] + args.seeds,
        context)

    ##############################################
    # graph of progress of collect job over time #
    ##############################################
    make_job(
        jinja_env,
        batch_run_dir,
        'mpistat_progress',
        [collector_job_id],
        context,
        collector_job_id)

    ###########################
    # create the cickhouse db #
    ###########################
    click_create_job_id = make_job(
        jinja_env,
        batch_run_dir,
        'mpistat_click_create',
        [],
        context,
        collector_job_id)

    #############################
    # load the data into the db #
    #############################
    make_job(
        jinja_env,
        batch_run_dir,
        'mpistat_click_load',
        sys.argv,
        context,
        click_create_job_id)
    logging.info('finished')
    return 0


if __name__ == "__main__":
    sys.exit(main())
