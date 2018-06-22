'''
Author: Jorge Diaz Jr

    Exercise 6
    Can you now turn your result from exercise 4 into an object?

    The idea is to run:
    myreader = ANReader(filename, chunk_size=1024)
    # get the 10th chunk of 1024 rows as a pandas dataframe
    row = myreader(10)

'''
import pandas as pd
import os

fc = 7.62939453125e-05
adc2counts = lambda x: ((int(x, 16) >> 8) - 0x40000) * fc \
        if (int(x, 16) >> 8) > 0x1FFFF else (int(x, 16) >> 8)*fc

class ANREADER:	
    '''
    resource_path and chunk size are arguments passed in
    returns a dataframe object where the dataframe is a specified
    chunk of data
    '''
    def __init__(self, resource_path, chunk_size=1024):
        '''
        adds the chunks of data to a list for file selected

        Parameters
        ----------
        resource_path: str
            path to file in which user desires to extract data from
        chunk_size: int
            the chunk of data in file that user selected

        Returns
        -------
        '''
        self.chunks_of_data = []
        for chunk in pd.read_csv(resource_path, delimiter = " ", names = ['time (s)', 'time (ns)', 'index', 'counts'], chunksize=chunk_size, header=None):
            chunk['volts'] = chunk['counts'].apply(adc2counts)
            chunk['total time (s)'] = chunk['time (s)'] + 1e-9*chunk['time (ns)']
            chunk = chunk.drop(columns = ['time (s)', 'time (ns)', 'counts'])
            chunk = chunk[['total time (s)', 'index', 'volts']]
            self.chunks_of_data.append(chunk)

    def __call__(self, chunk_num):
        '''
        returns specified chunk number/index from list of all chunks created

        Parameters
        ----------
        chunk_num: int
            chunk choice or row choice from data

        Returns
        -------
        result: dataframe object
            the dataframe chunk or subset 
        '''
        result = self.chunks_of_data[chunk_num]
        return result

    def __len__(self):
        '''
        Returns
        ------- 
        len: int
            the number of chunks for specific file
        '''
        return len(self.chunks_of_data)

def file_choice():
    '''
    user gets to select which file they want to work with
    
    Returns 
    -------
    file_choice: int
        index position of file in list
    '''
    print('Which file would you like to look at?')
    index = 1
    for file in an_files_only:
        print(str(index) + ':' + file)
        index += 1
    return int(input("Enter number: "))

def chunk_choice(myreader, file):
    '''
    user will need to enter which chunk number is desired from the data
    
    Returns
    ------- 
    chunk choice: int
        the chunk/row number desired in data set
    '''
    print('There are {0} chunks in {1}.'.format(len(myreader), file))
    return int(input("Which chunk number will you want, choose between 0 - {}:\nChunk choice: ".format(len(myreader) - 1)))


file_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/"
files_only = next(os.walk(file_path))[2]
an_files_only = [file for file in files_only if file.startswith("an_")]	

# user will enter file want to work with
# user will get to enter what chunk number desired
# dataframe is returned 

# subtracting 1 since indices start at 0
file_choice = file_choice() - 1

# creates object from specified file choice
myreader = ANREADER(file_path + an_files_only[file_choice], chunk_size=1024)

# this is the users choice for chunk number or row number from data
chunk_choice = chunk_choice(myreader, an_files_only[file_choice])

# creating a new object that holds dataframe such that the
# data frame is the specified chunk_number
newreader = myreader(chunk_choice)
print(newreader)
