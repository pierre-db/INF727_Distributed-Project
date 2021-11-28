from re import VERBOSE
import subprocess
import multiprocessing
import sys

# we define the username and workers filename as a global variable
USERNAME = 'dalbianco-20'
WORKERS_FILE = 'machines.txt'

# This function launches a sub process
def launch_subprocess(username, machine, command):
    # the command to be executed, contains two commands: one to create the remote dir and one to copy the files
    if command == 'mkdir':
        # we're using the -q otpion for quiet mode
        commands = ['ssh', '-q', username+'@'+machine,'mkdir -p /tmp/'+username]
    elif command == 'scp':
        commands = ['scp', '-q', 'SLAVE.py', username+'@'+machine+':/tmp/'+username]
    # simply create a sub process and return it
    if VERBOSE:
        print('{}: starting {} ...'.format(machine, command))
    process = subprocess.Popen(                    
                    commands,
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    universal_newlines=True, # to improve the output rendering
                     )
    return process

# Executes a command on a worker and checks that it worked
def execute_command(worker, command):
    timeout = 10
    try:
        # execute the sub process on a remote machine with a certain timeout
        process = launch_subprocess(USERNAME, worker, command)
        # try to get the process' outputs
        stdout, stderr = process.communicate(timeout=timeout)
        returncode = process.returncode

        # executed with errors
        if(returncode != 0):
            if VERBOSE:
                print('{}: \033[93m{} terminated with code {}\033[0m'.format(worker, command, returncode), file=sys.stderr)
                if(len(stderr) > 0):
                    print(' └ Errors: {}'.format(stderr.strip()))

        return returncode
    # the command timed out
    except subprocess.TimeoutExpired:
        if VERBOSE:
            print('{}: \033[91m{} timed out\033[0m'.format(worker, command), file=sys.stderr)
        process.kill()
        return -1
    # we have another unexpected exception
    except Exception as e:
        if VERBOSE:
            print('{}: {} \033[91mthrew an exception {}\033[0m'.format(worker, command, e), file=sys.stderr)
        process.kill()
        return -1

# Tries to execute mkdir and then scp on workers
def deploy_worker(worker):
    mkdir_returncode = execute_command(worker, 'mkdir')
    if mkdir_returncode == 0:
        if VERBOSE:
            print('{}: \033[92mmkdir done\033[0m'.format(worker))
        scp_returncode = execute_command(worker, 'scp')
        if scp_returncode == 0:
            if VERBOSE:
                print('{}: \033[92mscp done\033[0m'.format(worker))

            # the deployement is successful
            return True
    
    # the deployment failed
    return False
        

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
def deploy(verbose=False):
    global VERBOSE
    VERBOSE = verbose

    workers = read_workers('machines.txt')
    nb_workers = len(workers)
    if nb_workers == 0:
        print('No workers listed in workers file')
        return
    # we use multiprocessing to check each machine on an individual process
    pool = multiprocessing.Pool(len(workers))

    # the pool of processes executes the subprocess on remote machines
    workers_status = pool.map(deploy_worker, workers)
    
    # if we have some failures
    if not all(workers_status):
        # we generate a list of the failed workers
        failed_workers = [w for w, s in zip(workers, workers_status) if not s]

        new_lines = []
        with open(WORKERS_FILE) as f:
            # we read the file by line
            for line in f:
                #if it's a comment or not a failed worker, we add it as is
                if line.startswith('#') or line.strip() not in failed_workers:
                    new_lines.append(line)
                # if it's a failed worker, we comment it
                else:
                    new_lines.append('#'+line)

        # we re-wright the content of the file
        with open(WORKERS_FILE, 'w') as f:
            f.writelines(new_lines)
        
        # we print a message explaining what has been done
        print('Failed to deploy on {}. These lines have been commented out in the \'workers\' file.'.format(failed_workers))
    
    else:
        print('\033[92mDEPLOY FINISHED\033[0m')

if __name__ == '__main__':
    deploy(verbose=True)