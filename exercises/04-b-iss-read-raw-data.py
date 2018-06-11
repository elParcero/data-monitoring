'''
    Exercise 4 b)
    Can you now turn your result from exercise 4 into an object?

    The idea is to run:
    myreader = ANReader(filename, chunk_size=1024)
    # get the 10th chunk of 1024 rows as a pandas dataframe
    row = myreader(10)


'''

class ANReader:
    def __init__(self, filepath, chunk_size=1024):
        # load the file or save the filepath
        pass

    def __call__(self, chunk_number):
        # grab the nth chunk where the data is read in chunk_size size rows
        return result
