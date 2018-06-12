'''
    Exercise 6
    Can you now turn your result from exercise 4 into an object?

    The idea is to run:
    myreader = ANReader(filename, chunk_size=1024)
    # get the 10th chunk of 1024 rows as a pandas dataframe
    row = myreader(10)

    1. file handler
    2. get_file_list
    3. get_file_sizes

    When we read files using databroker, we're basically using objects like
    this to open and close the files for us. More on that later.

'''

class ANReader:
    def __init__(self, filepath, chunk_size=1024):
        # load the file or save the filepath
        pass

    def __call__(self, chunk_number):
        # grab the nth chunk where the data is read in chunk_size size rows
        return result
