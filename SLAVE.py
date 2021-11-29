import sys
import re
import os
import hashlib
import time
import subprocess
import multiprocessing

# we define the a few global variables for constant values
DEFAULTHOST = 'tp-4b01-@@'
WORKERS_FILE = 'machines.txt'
USERNAME = 'dalbianco-20'
MAXRETRY = 5

# This function launches a sub process
def launch_subprocess(worker, command, hostname=None):
    # the command to be executed, contains two commands: one to create the remote dir and one to copy the files
    if command == 'mkdir':
        # we're using the -q otpion for quiet mode
        commands = ['ssh', '-q', USERNAME+'@'+worker,'mkdir -p /tmp/'+USERNAME+'/shufflesreceived']
    elif command == 'scp':
        commands = ['scp', '-qC', '/tmp/'+USERNAME+'/shuffles/'+worker+'-'+hostname+'.txt', USERNAME+'@'+worker+':/tmp/'+USERNAME+'/shufflesreceived/']

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
def execute_command(worker, command, hostname=None):
    # we need a long timeout for the scp of the shuffles
    if command == 'scp':
        timeout = None
    else:
        timeout = None
    try:
        # execute the sub process on a remote machine with a certain timeout
        process = launch_subprocess(worker, command, hostname)
        # try to get the process' outputs
        stdout, stderr = process.communicate(timeout=timeout)
        returncode = process.returncode

        # executed with errors
        if(returncode != 0):
            print('{}: \033[93m{} terminated with code {}\033[0m'.format(worker, command, returncode))
            if(len(stderr) > 0):
                print(' └ Errors: {}'.format(stderr.strip()))
            if(len(stdout) > 0):
                print(' └ Output: {}'.format(stdout.strip()))

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

# the words maps function
def get_maps(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            words_map = []

            # we read the file by line
            for line in f:
                 # we split the line by whitespace
                words = line.split()

                # we map the words list to the correct format
                # and we add it to our words_map list
                words_map.extend(map(lambda w: '{} 1\n'.format(w), words))
    
    except Exception:
        return None
    
    return words_map

# the words shuffle function
def get_shuffles(filename, workers, nb_workers, curdir, hostname):
    try:
        # we read the content of the file created during the map
        with open(filename, 'r') as f:
            for line in f:
                # we generate the hash from each lines
                hash = generate_hash(line.split(' ')[0])
                #hashes.append(hash)
                
                # we perform attribution per machine using the hash
                modulo = int(hash) % nb_workers
                
                # # we write the content in a file in shuffles/<worker>/<hash>-<hostname>.txt
                # with open(curdir+'/shuffles/'+workers[modulo]+'/'+hash+'-'+hostname+'.txt', 'a') as f:
                #     f.writelines(line)

                # we write the content in a file in shuffles/<worker>-<hostname>.txt
                with open(curdir+'/shuffles/'+workers[modulo]+'-'+hostname+'.txt', 'a') as f:
                    f.writelines(line)
    
    except Exception:
        return False
    
    return True

# Tries to execute commands on workers
def send_shuffle(worker_hostname_curdir):
    worker, (hostname, curdir) = worker_hostname_curdir
    # we check if there is a file for this worker, if not, we do nothing
    if(not os.path.exists(curdir+'/shuffles/'+worker+'-'+hostname+'.txt')):
        return True
    
    # we execute this MAXRETRY times or until we succeed
    for i in range(MAXRETRY):
        mkdir_returncode = execute_command(worker, 'mkdir')
        if mkdir_returncode == 0:
            scp_s_returncode = execute_command(worker, 'scp', hostname)
            if scp_s_returncode == 0:
                print('{}: \033[92mjob done\033[0m'.format(worker))
                #the job is successful
                return True
        
        # we wait 
        time.sleep(0.2*(i+1))
    
    # the job failed
    return False

# the words reduce function
def get_reduces(filename, words_count):
    try:
        # we open the file
        with open(filename, encoding='utf-8') as f:
            # we read each line
            for line in f:
                # we get the word
                word = line.split(' ')[0]
                # we update our dict accordingly
                words_count[word] = words_count.get(word, 0) + 1

    except Exception:
        return False
    
    # we return our dict
    return True

# a function that generates a unique hash
def generate_hash(s):
    return str(int.from_bytes(hashlib.sha256(str.encode(s)).digest()[:4], 'little'))

def make_dir(dirname):
    try:
        os.makedirs(dirname, exist_ok=True)
    except FileExistsError:
        pass
    except OSError:
        # if we have an error while creating we exit with an error
        print('Not able to create directory maps', file=sys.stderr)
        return False
    
    return True

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

##
# main function
##
def main():
    # we check that we received the expected number of arguments
    if len(sys.argv) < 2:
        # we exit with an error if we didn't get the expected number of arguments
        print('Expected at least 1 argument, received none', file=sys.stderr)
        sys.exit(1)
    
    # we parse the option passed in argument
    option = sys.argv[1]

    if option in ['0', '1']:
        if len(sys.argv) < 3:
            # we exit with an error if we didn't get the expected number of arguments
            print('Expected at 2 arguments, received {}'.format(len(sys.argv)), file=sys.stderr)
            sys.exit(1)
        else: # we get the filename passed in arguement
            file = sys.argv[2]
    
    # we get our current working directory
    curdir = os.path.dirname(sys.argv[0])

    # if the current directory is empty, we replace it by '.' to avoid issues later
    if curdir == '':
        curdir = '.'

    ###
    # 0: get the splits and map
    if option == '0':
        # we extract the file number from the filename
        file_nb = re.search('([0-9]+)\.txt', file, re.IGNORECASE).group(1)
        
        # we perform the words map
        words_map = get_maps(file)

        # we couldn't open the file
        if words_map == None:
            print('Couldn\'t open file: {}'.format(file), file=sys.stderr)
            sys.exit(1)

        # we create the maps directory
        if not make_dir(curdir+'/maps'):
            # if we have an error while creating we exit with an error
            sys.exit(1)

        # we write the content in a file UM#id.txt
        with open(curdir+'/maps/UM'+file_nb+'.txt', 'w') as f:
            f.writelines(words_map)
    ###
    # option 1: shuffle
    elif option == '1':
        try:
            hostname = os.uname()[1]
        except Exception:
            hostname = DEFAULTHOST

        workers = read_workers(curdir+'/'+WORKERS_FILE)
        nb_workers = len(workers)

        # we create the shuffles directory
        if not make_dir(curdir+'/shuffles'):
            # if we have an error while creating we exit with an error
            sys.exit(1)
        
        # to create a directory per worker
        # for worker in workers:
        #     if not make_dir(curdir+'/shuffles/'+worker):
        #         # if we have an error while creating we exit with an error
        #         sys.exit(1)

        shuffle_done = get_shuffles(file, workers, nb_workers, curdir, hostname)
        # we couldn't open the file
        if not shuffle_done:
            print('Couldn\'t open file: {}'.format(file), file=sys.stderr)
            sys.exit(2)
        
        # we generate a list of tuples (worker, filename) that attributes a worker to each filename
        pool = multiprocessing.Pool(nb_workers)
        # we determine which worker we need to send the file to
        workers_status = pool.map(send_shuffle, zip(workers, [(hostname,curdir)]*nb_workers))
        # if we have some failures
        if not all(workers_status):
            # we generate a list of the failed workers
            failed_workers = [w for w, s in zip(workers, workers_status) if not s]
        
            # we print a message detailing which machines failed
            print('Failed shuffle transfer to {}'.format(failed_workers), file=sys.stderr)
            sys.exit(3)

    ###
    # option 2: reduce
    elif option == '2':
        try:
            hostname = os.uname()[1]
        except Exception:
            hostname = DEFAULTHOST
        # we create the reduces directory
        if not make_dir(curdir+'/reduces'):
            # if we have an error while creating we exit with an error
            sys.exit(1)
        
        # we loop on each file received in shufflesreceived/
        words_count = {}
        for f in os.listdir(curdir+'/shufflesreceived/'):
            file = curdir+'/shufflesreceived/'+f
            #hash = f.split('-')[0]

            reduce_done = get_reduces(file, words_count)
            # we couldn't open the file
            if not reduce_done:
                print('Couldn\'t open file: {}'.format(file), file=sys.stderr)
                sys.exit(1)
        
        # we write our words count into a file in /reduces
        for w, c in words_count.items():
            with open(curdir+'/reduces/'+hostname+'.txt', 'a') as f:
                f.writelines('{} {}\n'.format(w, c))

        # # code to create one hash.txt per word
        # for (w,h), c in words_count.items():
        #     # we write the content in a file <hash>.txt in reduces/
        #     with open(curdir+'/reduces/'+h+'.txt', 'w') as f:
        #         f.writelines('{} {}\n'.format(w, c))

if __name__ == '__main__':
    main()  