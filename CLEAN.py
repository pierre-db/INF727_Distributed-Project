from re import VERBOSE
import subprocess
import multiprocessing
import os
import sys

# we define the username and workers filename as a global variable
USERNAME = 'dalbianco-20'
WORKERS_FILE = 'machines.txt'

# This function launches a sub process
def launch_subprocess(machine):
    # simply create a sub process and return it
    if VERBOSE:
        print('{}: starting clean up ...'.format(machine))
    process = subprocess.Popen(                    
                     ['ssh', '-q', USERNAME+'@'+machine,'rm -rf /tmp/'+USERNAME],
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    universal_newlines=True, # to improve the output rendering
                     )
    return process

# Executes a command on a worker and checks that it worked
def execute_clean(worker):
    timeout = 15
    try:
        # execute the sub process on a remote machine with a certain timeout
        process = launch_subprocess(worker)
        # try to get the process' outputs
        _, stderr = process.communicate(timeout=timeout)
        returncode = process.returncode

        # executed with no errors
        if returncode == 0:
            if VERBOSE:
                print('{}: \033[92mclean OK\033[0m'.format(worker))
        else:
            if VERBOSE:
                print('{}: \033[93mterminated with code {}\033[0m'.format(worker, returncode), file=sys.stderr)
                if(len(stderr) > 0):
                    print(' └ Errors: {}'.format(stderr.strip()))
    # the command timed out
    except subprocess.TimeoutExpired:
        if VERBOSE:
            print('{}: \033[91mtimed out\033[0m'.format(worker), file=sys.stderr)
        process.kill()
    # we have another unexpected exception
    except Exception as e:
        if VERBOSE:
            print('{}: \033[91mthrew an exception {}\033[0m'.format(worker, e), file=sys.stderr)
        process.kill()

def read_workers(filename):
    workers = []
    with open(filename) as f:
        # we read the file by line
        for line in f:
            # we don't read lines that start with a # to allow for comments
            if line.startswith('#'):
                continue
            # we add the worker to the list of workers
            workers.append(line.strip())
    return workers

# Main function
def clean(verbose=False):
    global VERBOSE
    VERBOSE = verbose

    ###
    # Clean workers
    ###
    workers = read_workers(WORKERS_FILE)

    # we use multiprocessing to check each machine on an individual process
    pool = multiprocessing.Pool(len(workers))
    # the pool of processes executes the subprocess on remote machines
    pool.map(execute_clean, workers)

    ###
    # Clean master
    ###
    # we get our current working directory
    curdir = os.path.dirname(sys.argv[0])

    # if the current directory is empty, we replace it by '.' to avoid issues later
    if curdir == '':
        curdir = '.'

    # we clean our local files
    if os.path.exists(curdir+'/splits'):
        for f in os.listdir(curdir+'/splits'):
            os.remove(curdir+'/splits/'+f)
        os.rmdir(curdir+'/splits')

    # we also clean the local reduces folder
    if os.path.exists(curdir+'/reduces'):
        for f in os.listdir(curdir+'/reduces'):
            os.remove(curdir+'/reduces/'+f)
        os.rmdir(curdir+'/reduces')
    

    print('\033[92mCLEAN FINISHED\033[0m')


if __name__ == '__main__':
    clean(verbose=True)