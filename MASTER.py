# https://remisharrock.fr/courses/simple-hadoop-mapreduce-from-scratch/

import subprocess
import multiprocessing
import time
import sys
import re
import os

import CLEAN as clean
import DEPLOY as deploy

# we define the username and workers filename as a global variable
USERNAME = 'dalbianco-20'
WORKERS_FILE = 'machines.txt'
#MAXRETRY = 3

# This function launches a sub process
def launch_subprocess(worker, command, file=None):
    # the command to be executed, contains two commands: one to create the remote dir and one to copy the files
    if command == 'mkdir':
        # we're using the -q otpion for quiet mode
        commands = ['ssh', '-q', USERNAME+'@'+worker,'mkdir -p /tmp/'+USERNAME+'/splits']
    elif command == 'scp_split':
        commands = ['scp', '-qC', 'splits/'+file, USERNAME+'@'+worker+':/tmp/'+USERNAME+'/splits']
    elif command == 'scp_workers':
        commands = ['scp', '-qC', WORKERS_FILE, USERNAME+'@'+worker+':/tmp/'+USERNAME+'/machines.txt']
    elif command == 'map':
        commands = ['ssh','-q', USERNAME+'@'+worker, 'python3 /tmp/'+USERNAME+'/SLAVE.py 0 /tmp/'+USERNAME+'/splits/'+file]
    elif command == 'shuffle':
        commands = ['ssh','-q', USERNAME+'@'+worker, 'python3 /tmp/'+USERNAME+'/SLAVE.py 1 /tmp/'+USERNAME+'/maps/'+file]
    elif command == 'reduce':
        commands = ['ssh','-q', USERNAME+'@'+worker, 'python3 /tmp/'+USERNAME+'/SLAVE.py 2']
    elif command == 'fetch':
        commands = ['scp', '-qC', USERNAME+'@'+worker+':/tmp/'+USERNAME+'/reduces/*', 'reduces/']
    
    # simply create a sub process and return it
    #print('{}: starting {} ...'.format(worker, command))
    process = subprocess.Popen(                    
                    commands,
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    universal_newlines=True, # to improve the output rendering
                     )
    return process

# Executes a command on a worker and checks that it worked
def execute_command(worker, command, file=None):
    if command == 'mkdir':
        timeout = None
    else:
        timeout = None

    try:
        # execute the sub process on a remote machine with a certain timeout
        process = launch_subprocess(worker, command, file)
        # try to get the process' outputs
        stdout, stderr = process.communicate(timeout=timeout)
        returncode = process.returncode

        # executed with errors
        if(returncode != 0):
            print('{}: \033[93m{} terminated with code {}\033[0m'.format(worker, command, returncode))
            if(len(stderr) > 0):
                print(' └ Errors: {}'.format(stderr.strip()))

        return returncode
    # the command timed out
    except subprocess.TimeoutExpired:
        print('{}: \033[91m{} timed out\033[0m'.format(worker, command))
        process.kill()
        return -1
    # we have another unexpected exception
    except Exception as e:
        print('{}: {} \033[91mthrew an exception {}\033[0m'.format(worker, command, e))
        process.kill()
        return -1

# Tries to execute commands on workers
def launch_map(worker_file):
    worker, file = worker_file
    mkdir_returncode = execute_command(worker, 'mkdir')
    if mkdir_returncode == 0:
        scp_s_returncode = execute_command(worker, 'scp_split', file)
        if scp_s_returncode == 0:
            scp_w_returncode = execute_command(worker, 'scp_workers')
            if scp_w_returncode == 0:
                map_returncode = execute_command(worker, 'map', file)
                if map_returncode == 0:
                    #print('{}: \033[92mjob done\033[0m'.format(worker))

                    # the job is successful
                    return True
    
    # the job failed
    return False

# Tries to execute commands on workers
def launch_shuffle(worker_file):
    worker, file = worker_file
    # we extract the file number from the filename
    file_nb = re.search('([0-9]+)\.txt', file, re.IGNORECASE).group(1)
    shuffle_returncode = execute_command(worker, 'shuffle', 'UM'+file_nb+'.txt')
    if shuffle_returncode == 0:
        # the job is successful
        return True
    
    # the job failed
    return False

# Tries to execute commands on workers
def launch_reduce(worker):
    reduce_returncode = execute_command(worker, 'reduce')
    if reduce_returncode == 0:
        # the job is successful
        return True
    
    # the job failed
    return False

# Tries to execute commands on workers
def launch_fetch_results(worker):
    if make_dir('reduces'):
        fetch_returncode = execute_command(worker, 'fetch')
        if fetch_returncode == 0:
            # the job is successful
            return True
    
    # the job failed
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

def make_dir(dirname):
    try:
        os.makedirs(dirname)
    except FileExistsError:
        pass
    except OSError:
        # if we have an error while creating we exit with an error
        print('Not able to create directory maps', file=sys.stderr)
        return False
    
    return True

def perform_split(filename, workers):
    # we create the splits directory
    if not make_dir('splits'):
        # if we have an error while creating we exit with an error
        sys.exit(1)
    try:
        # we define the size in bytes of the file to split
        size =  os.path.getsize(filename)

        # with open(filename, 'r') as f:
        #     size = os.fstat(f.fileno()).st_size
        #     size = len(f.read())
        # we define the block size
        nb_workers = len(workers)
        block_size = size // (nb_workers)
        block_nb = 0

        #print(size, nb_workers, block_size)
        # a list that will contain the split files
        split_files = []

    # we'll perform the split
        with open(filename, 'r') as f:
            split_file = 'S'+str(block_nb)+'.txt'
            split_files.append(split_file)

            # we read by block_size
            block = f.read(block_size)
            while block:
                # we keep reading until we find a space
                char = block[-1]
                while char not in (' ', '\n'):
                    # we add the char to the current block
                    char = f.read(1)
                    block += char
                    # we arrived at the end of the file
                    if not char:
                        break
                
                # we have a block of approx. block_size that ends with a space
                # we write it to the matching file
                with open('splits/'+split_file, 'w') as s:
                    s.write(block)

                # next block
                block = f.read(block_size)
                block_nb += 1
                split_file = 'S'+str(block_nb)+'.txt'
                split_files.append(split_file)
                # necessary to write the last file in case it is empty
                if len(block) == 0 and block_nb < nb_workers: block = ' '

    
    except FileNotFoundError:
        print('File not found: {}'.format(filename), file=sys.stderr)
        return None

    # we have another unexpected exception
    except Exception as e:
        print('Failed on split: {}'.format(e), file=sys.stderr)
        return None
    
    # we return the list of files we generated
    return split_files

# read and print our results
def read_results():
    words_count = []

    # we read all files from the reduces folder
    for file in os.listdir('reduces/'):
            with open('reduces/'+file) as f:
                # we add each count in a list
                for line in f:
                    w, c = line.split(' ')
                    words_count.append((w, int(c)))
    
    words_count = sorted(words_count, key=lambda e: e[1], reverse=True)

    return words_count

# Main function
def main():
    ###
    # INITIALIZATION
    ###
    if len(sys.argv) < 2:
        # we exit with an error if we didn't get the expected number of arguments
        print('Expected at least 1 argument, received none', file=sys.stderr)
        sys.exit(1)
    
    # we parse the option passed in argument
    filename = sys.argv[1]
    ###
    # CLEAN
    ###
    clean.clean(verbose=False)

    ###
    # DEPLOY
    ###
    deploy.deploy(verbose=False)

    # we create the splits directory
    if not make_dir('splits'):
        # if we have an error while creating we exit with an error
        sys.exit(1)
    
    # we read our list of workers
    workers = read_workers(WORKERS_FILE)


    ###
    # SPLIT
    ###
    start_time = time.time()
    print('Starting split ...')
    # we perform the split
    split_files = perform_split(filename, workers)

    if not split_files:
        # we print a message explaining what has been done
        print('Failed split')
        sys.exit(1)
    
    total_time = time.time() - start_time
    print('\033[92mSPLIT FINISHED\033[0m in {:.2f} s'.format(total_time))

    ###
    # MAP
    ###
    start_time = time.time()
    print('Starting map ...')
    # we use multiprocessing to check each machine on an individual process
    pool = multiprocessing.Pool(len(workers))

    # the pool of processes executes the subprocess on remote machines
    workers_status = pool.map(launch_map, zip(workers, split_files))
    
    # if we have some failures
    if not all(workers_status):
        # we generate a list of the failed workers
        failed_workers = [w for w, s in zip(workers, workers_status) if not s]
        
        # we print a message explaining what has been done
        print('Failed map on {}'.format(failed_workers))
        sys.exit(1)
    
    # all maps finished successfully
    total_time = time.time() - start_time
    print('\033[92mMAP FINISHED\033[0m in {:.2f} s'.format(total_time))

    ###
    # SHUFFLE
    ###
    start_time = time.time()
    print('Starting shuffle ...')
    # the pool of processes executes the subprocess on remote machines
    workers_status = pool.map(launch_shuffle, zip(workers, split_files))

    # to retry the suffle MAXRETRY times in case of a failure
    # for _ in range(MAXRETRY):
    #     if not all(workers_status):
    #         # we generate a list of the failed workers
    #         failed_workers = [w for w, s in zip(workers, workers_status) if not s]
    #         failed_files = [w for w, s in zip(split_files, workers_status) if not s]

    #         # we print a message explaining what has been done
    #         print('Failed shuffle on {}'.format(failed_workers))
    #         print('Retrying ...')

    #         workers_status = pool.map(launch_shuffle, zip(failed_workers, failed_files))
    
    # if we have some failures
    if not all(workers_status):
        failed_workers = [w for w, s in zip(workers, workers_status) if not s]
        print('Failed shuffle on {}'.format(failed_workers))
        sys.exit(1)

    # all shuffles finished successfully
    total_time = time.time() - start_time
    print('\033[92mSUFFLE FINISHED\033[0m in {:.2f} s'.format(total_time))

    ###
    # REDUCE
    ###
    start_time = time.time()
    print('Starting reduce ...')
    # the pool of processes executes the subprocess on remote machines
    workers_status = pool.map(launch_reduce, workers)

    # if we have some failures
    if not all(workers_status):
        # we generate a list of the failed workers
        failed_workers = [w for w, s in zip(workers, workers_status) if not s]
        
        # we print a message explaining what has been done
        print('Failed shuffle on {}'.format(failed_workers))
        sys.exit(1)

    # all reduces finished successfully
    total_time = time.time() - start_time
    print('\033[92mREDUCE FINISHED\033[0m in {:.2f} s'.format(total_time))
    
    ###
    # FETCH RESULTS
    ###
    start_time = time.time()
    print('Starting fetch ...')
    # the pool of processes executes the subprocess on remote machines
    workers_status = pool.map(launch_fetch_results, workers)

    # if we have some failures
    if not all(workers_status):
        # we generate a list of the failed workers
        failed_workers = [w for w, s in zip(workers, workers_status) if not s]
        
        # we print a message explaining what has been done
        print('Failed fetching results on {}'.format(failed_workers))
        sys.exit(1)

    # all shuffles finished successfully
    total_time = time.time() - start_time
    print('\033[92mFETCHED ALL RESULTS\033[0m in {:.2f} s'.format(total_time))

    words_count = read_results()
    with open('output.txt', 'w') as f:
        for (w, c) in words_count:
            f.writelines('{} {}\n'.format(w, c))

    print('Results written into output.txt')

if __name__ == '__main__':
    main()