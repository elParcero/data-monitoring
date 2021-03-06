'''
Author: Jorge Diaz Jr
Exercise 7
File Handler
'''
import uuid
import pandas as pd
import os

fc = 7.62939453125e-05
adc2counts = lambda x: ((int(x, 16) >> 8) - 0x40000) * fc \
        if (int(x, 16) >> 8) > 0x1FFFF else (int(x, 16) >> 8)*fc
enc2counts = lambda x: int(x) if int(x) <= 0 else -(int(x) ^ 0xffffff - 1)


class ANREADER:
    '''
    resource_path and chunk size are arguments passed in
    returns a dataframe object where the dataframe is a specified
    chunk of data
    '''
    def __init__(self, resource_path, chunk_size = 1024):
        '''
        reads file in and converts each chunk into a dataframe object
        
        Parameters
        ----------
        resource_path: str
            user provides path to file in which they will be working with
        chunk_size: int
            the size of the chunk of data for each file read in
        
        Returns
        -------
        '''
        self.chunks_of_data = []
        for chunk in pd.read_csv(resource_path, delimiter=" ", names = ['time (s)', 'time (ns)', 'index', 'counts'], chunksize=chunk_size, header=None):
            chunk['volts'] = chunk['counts'].apply(adc2counts)
            chunk['total time (s)'] = chunk['time (s)'] + 1e-9*chunk['time (ns)']
            chunk.drop(labels = ['counts', 'time (s)', 'time (ns)'], axis = 1, inplace = True)
            cols = chunk.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            chunk = chunk[cols]
            self.chunks_of_data.append(chunk)   
        

    def __call__(self, chunk_num):
        '''
        Parameters
        ----------
        chunk_num: int
            user specifies chunk number to be retrieved from data
        
        Returns
        -------
        chunk_of_data: dataframe object
            the chunk of data is retrieved from the list holding the chunks
        '''
        result = self.chunks_of_data[chunk_num]
        return result


    def __len__(self):
        '''
        Returns
        -------
        the number of chunks for specific file: int
        '''
        return len(self.chunks_of_data)


def get_resource(resource_uid, filepath, filename):
    '''
    returns a resource document for file

    Parameters
    ----------
    resource_uid: str
        resource_uid will be a unique identifier for the resource document that 
        will be created
    filepath: str
        path to file
    filename: str
        the file that is going to be worked with inside the filepath

    Returns
    -------
    resource: dict
        dictionary that contains specific key,val arguments that relates to file
    '''
    resource = {'id': resource_uid,
    'path_semantics': 'posix',
    'resource_path': filepath + filename,
    'root': '/home/jdiaz',
    'resource_kwargs': {'chunk_size': 1024},
    'spec': 'PIZZABOX_AN_FILE_TXT',
    'uid': resource_uid,
    }
    return resource


def get_datum(datum_uid, resource_uid):
    '''
    returns a datum document for file

    Parameters
    ----------
    datum_uid: str
        datum_uid will be a unique identifier for the datum document that 
        will be created
    resource_uid: str
        the same resource uid for the file being worked with, datum doc will be able to 
        point to resource document

    Returns
    -------
    datum: dict
        dictionary that contains specific key,val arguments that relates to file
    '''
    datum = {'datum_id': datum_uid,
    'datum_kwargs': {'chunk_num': 2},
    'resource': resource_uid
    }
    return datum


def create_resource_datum(filenames):
    '''
    creates the resource and datum documents 
    
    Parameters
    ----------
    filenames: list
        each index is a name of file in directory being worked with
    
    Returns
    -------
    resources: list
        each position holds the resource doc for specific file being worked with
    datums: list
        each position holds the datum doc for specific file being worked with
    '''
    resources = []
    datums = []

    for i in range(len(filenames)):
        resource_uid = str(uuid.uuid4())
        datum_uid = str(uuid.uuid4())
        resource = get_resource(resource_uid, filepath, filenames[i])
        datum = get_datum(datum_uid, resource['uid'])
        resources.append(resource)
        datums.append(datum)
    return resources, datums


filepath = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/"
filenames = os.listdir(filepath)
filenames = [file for file in filenames if file.startswith('an')]

resources, datums = create_resource_datum(filenames)

def user_chooses_file(filenames):
    '''
    uer chooses which file to work with

    Parameters
    ----------
    filenames: list
        list that contains all file names such 
        that user can choose which one to work with

    Returns
    -------
    file_choice: int
        number which indexes one of the files from the filenames list
    '''
    print("Which file would you like to work with?")
    for i in range(len(filenames)):
        print(str(i+1) + ". " + filenames[i])
    file_choice = int(input('Choose number: ')) - 1
    return file_choice

file_choice = user_chooses_file(filenames)

# you would read it something like this
# fh is an object from the ANREADER class
# data uses the object created to invoke the __call__ method
fh = ANREADER(resources[file_choice]['resource_path'], **resources[file_choice]['resource_kwargs'])
data = fh(**datums[file_choice]['datum_kwargs'])
