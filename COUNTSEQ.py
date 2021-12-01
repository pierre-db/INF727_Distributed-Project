import sys
import time
import argparse

# Count the number of words occurence in the input file
def get_words_count(filename, sortby):
    with open(filename, encoding='utf-8') as f:
        words_count = {}

        # we read the file by line
        for line in f:
            words = line.split() # we split the line by whitespace

            for w in words:
                if w in words_count:
                    words_count[w] += 1
                else:
                    words_count[w] = 1
        
        if sortby == 'alphabet': # to sort by alphabetical order
            words_count = dict(sorted(words_count.items()))
        elif sortby == 'occurences': # to sort by number of occurences and then alpabetical
            words_count = dict(sorted(words_count.items(), key=lambda x: (-x[1], x[0])))
    
    return words_count

# This function allows us to easilly parse our arguments
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sortby", "-sortby", default='occurences', nargs=1, 
                        help="""To sort the results by alphabetical order (alphabet), by occurences (occurences), or no sorting (none)""")
    parser.add_argument('filename', metavar='filename', help='The file to analyse')
    return parser.parse_args()

# Main function
# parses line argument to get the input file passed as argument
def main():
    # we parse arguments
    args = parse_arguments()

    # we extrac the arguments
    filename = args.filename
    option = args.sortby[0].lower()

    # to sort by alphabetical order
    if option in ['alphabetical','alphabet', 'alpha', 'a']: 
         sortby = 'alphabet'
    # to sort by number of occurences and then alpabetical
    elif option in ['occurences','occurence', 'occ', 'o']: 
         sortby = 'occurences'
     # to specify that we don't want any sorting
    elif option in ['none', 'n']:
         sortby = 'none'
    else:
         print('unknown sort option: \'' + option +'\' use \'alphabet\', \'occurences\' or \'none\'')
         sys.exit(1)
    
    # we start a timer
    start_time = time.time()
    words_count =  get_words_count(filename, sortby)

    # we calculate the execution time before the print
    total_time = (time.time() - start_time)
    
    # we calculate and print the execution time
    print('\033[92mCOUNT DONE\033[0m in: {:.2f} s'.format(total_time))

    # we print every words
    # for w in words_count:
    #     print('{} {}'.format(w, words_count[w]))

    # we write the resulats in a file
    with open('output.txt', 'w') as f:
        for (w, c) in words_count.items():
            f.writelines('{} {}\n'.format(w, c))

    print('Results written into output.txt')

if __name__ == '__main__':
    main()
