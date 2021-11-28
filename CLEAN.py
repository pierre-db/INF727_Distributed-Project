from re import VERBOSE
import subprocess
import multiprocessing
import os
import sys

# we define the username and workers filename as a global variable
USERNAME = 'dalbianco-20'
WORKERS_FILE = 'machines.txt'

# This function launches a sub process
def launch_subprocess(username, machine):
    # simply create a sub process and return it
    if VERBOSE:
        print('{}: starting clean up ...'.format(machine))
    process = subprocess.Popen(                    
                     ['ssh', '-q', username+'@'+machine,'rm -rf /tmp/'+username],
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
        process = launch_subprocess('dalbianco-20', worker)
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
    # we clean the local splits folder
    if os.path.exists('splits'):
        for f in os.listdir('splits'):
            os.remove('splits/'+f)
        os.rmdir('splits')

    # we also clean the local reduces folder
    if os.path.exists('reduces'):
        for f in os.listdir('reduces'):
            os.remove('reduces/'+f)
        os.rmdir('reduces')
    
    else:
        print('\033[92mCLEAN FINISHED\033[0m')


if __name__ == '__main__':
    clean(verbose=True)