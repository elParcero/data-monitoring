'''
Author: Jorge Diaz
Exercise 8
'''
import uuid
import pandas as pd
import os
from databroker.tests.utils import temp_config
from databroker import Broker
from databroker.assets.handlers_base import HandlerBase

# this will create a temporary databroker object with nothing in it
db = Broker.from_config(temp_config())

fc = 7.62939453125e-05
adc2counts = lambda x: ((int(x, 16) >> 8) - 0x40000) * fc \
        if (int(x, 16) >> 8) > 0x1FFFF else (int(x, 16) >> 8)*fc
enc2counts = lambda x: int(x) if int(x) <= 0 else -(int(x) ^ 0xffffff - 1)


class PizzaBoxAnHandler(HandlerBase):
    def __init__(self, resource_path, chunk_size=1024):
        '''
        adds the chunks of data to a list for specific file

        Parameters
        ----------
        resource_path: str
            tells the computer where to find the file
        chunk_size: int (optional)
            user specifices size of chunk for data, default is 1024
        '''
        self.chunks_of_data = []
        for chunk in pd.read_csv(resource_path, chunksize=chunk_size, 
                names =['time (s)', 'time (ns)', 'index', 'counts'], 
                delimiter = " ", header=None):
            chunk['adc'] = chunk['counts'].apply(adc2counts)
            chunk['timestamp'] = chunk['time (s)'] + 1e-9*chunk['time (ns)']
            chunk = chunk.drop(columns = ['time (s)', 'time (ns)', 'counts', 'index'])
            chunk = chunk[['timestamp','adc']]
            self.chunks_of_data.append(chunk)

    def __call__(self, chunk_num):
        '''
        Returns 
        -------
        result: dataframe object
            specified chunk number/index from list of all chunks created
        '''
        result = self.chunks_of_data[chunk_num]
        return result

class PizzaBoxEnHandler(HandlerBase):
    def __init__(self, resource_path, chunk_size=1024):
        '''
        adds the chunks of data to a list for specific file

        Parameters
        ----------
        resource_path: str
            tells the computer where to find the file
        chunk_size: int (optional)
            user specifices size of chunk for data, default is 1024
        '''
        self.chunks_of_data = []
        for chunk in pd.read_csv(resource_path, chunksize=chunk_size, 
                names = ['time (s)', 'time (ns)', 'encoder', 'index', 'di'], 
                delimiter = " ", header=None):
            chunk['timestamp'] = chunk['time (s)'] + 1e-9*chunk['time (ns)']
            chunk['encoder'] = chunk['encoder'].apply(enc2counts)
            chunk = chunk.drop(columns = ['time (s)', 'time (ns)', 'index', 'di'])
            chunk = chunk[['timestamp', 'encoder']]
            self.chunks_of_data.append(chunk)

    def __call__(self, chunk_num):
        '''
        Returns 
        -------
        result: dataframe object
            specified chunk number/index from list of all chunks created
        '''
        result = self.chunks_of_data[chunk_num]
        return result



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
    
    Returns
    -------
    resource: dict
        dictionary that contains specific key,val arguments that relates to file
    '''
    spec = ''
    if filename.startswith('an'):
        spec = 'PIZZABOX_AN_FILE_TXT'
    else:
        spec = 'PIZZABOX_EN_FILE_TXT'

    resource = {'id': resource_uid,
    'path_semantics': 'posix',
    'resource_path': filepath + filename,
    'root': '/home/jdiaz',
    'resource_kwargs': {},
    'spec': spec,
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
    'datum_kwargs': {'chunk_num' : 7},
    'resource': resource_uid
    }
    return datum


def gen_an_resource(resource_uid, fPath, an_filename):
    an_resource = get_resource(resource_uid, fPath, an_filename)
    return an_resource


def gen_an_datum(datum_uid, an_resource_uid):
    an_datum = get_datum(datum_uid, an_resource_uid)
    return an_datum


def gen_an_resources_datums(fPath, an_filenames):
    an_resources = []
    an_datums = []
    for i in range(len(an_filenames)):
        resource_uid = str(uuid.uuid4()) #create a unique id for resource
        datum_uid = str(uuid.uuid4()) #create a unique id for datum
        an_resource = gen_an_resource(resource_uid, fPath, an_filenames[i])
        an_datum = gen_an_datum(datum_uid, an_resource['uid'])
        an_resources.append(an_resource)
        an_datums.append(an_datum)

    return an_resources, an_datums


def register_an_resources_datums(an_resources, an_datums):
    new_resources = []
    new_datums = []
    for i in range(len(an_resources)):
        new_resource = db.reg.insert_resource( "PIZZABOX_AN_FILE_TXT", 
            resource_path = an_resources[i]['resource_path'], 
            resource_kwargs = an_resources[i]['resource_kwargs'], 
            uid = an_resources[i]['uid'])
        new_datum = db.reg.insert_datum(resource = new_resource, 
            datum_id = an_datums[i]['datum_id'],
            datum_kwargs = an_datums[i]['datum_kwargs'])

        new_resources.append(new_resource)
        new_datums.append(new_datum)
    return new_resources, new_datums


def register_an_resources_given_datum_id(new_an_resources, new_an_datums):
    registered_resources = []
    for i in range(len(new_an_resources)):
        resource = db.reg.resource_given_datum_id(new_an_datums[i]['datum_id'])
        registered_resources.append(resource)
    return registered_resources


def an_datums_generated_given_resources(new_an_datums):
    an_datums_generated = []
    for i in range(len(new_an_datums)):
        datum_gen = db.reg.datum_gen_given_resource(new_an_datums[i]['resource']) 
        an_datums_generated.append(next(datum_gen))
    return an_datums_generated


def gen_en_resource(resource_uid, fPath, en_filename):
    en_resource = get_resource(resource_uid, fPath, en_filename)
    return en_resource


def gen_en_datum(datum_uid, en_resource_uid):
    en_datum = get_datum(datum_uid, en_resource_uid)
    return en_datum


def gen_en_resources_datums(fPath, en_filenames):
    en_resources = []
    en_datums = []
    for i in range(len(en_filenames)):
        resource_uid = str(uuid.uuid4()) #create a unique id for resource
        datum_uid = str(uuid.uuid4()) #create a unique id for datum
        en_resource = gen_an_resource(resource_uid, fPath, en_filenames[i])
        en_datum = gen_an_datum(datum_uid, en_resource['uid'])
        en_resources.append(en_resource)
        en_datums.append(en_datum)

    return en_resources, en_datums


def register_en_resources_datums(en_resources, en_datums):
    new_resources = []
    new_datums = []
    for i in range(len(en_resources)):
        new_resource = db.reg.insert_resource( "PIZZABOX_EN_FILE_TXT", 
            resource_path = en_resources[i]['resource_path'], 
            resource_kwargs = en_resources[i]['resource_kwargs'], 
            uid = en_resources[i]['uid'])
        new_datum = db.reg.insert_datum(resource = new_resource, 
            datum_id = en_datums[i]['datum_id'],
            datum_kwargs = en_datums[i]['datum_kwargs'])

        new_resources.append(new_resource)
        new_datums.append(new_datum)
    return new_resources, new_datums


def register_en_resources_given_datum_id(new_en_resources, new_en_datums):
    registered_resources = []
    for i in range(len(new_en_resources)):
        resource = db.reg.resource_given_datum_id(new_en_datums[i]['datum_id'])
        registered_resources.append(resource)
    return registered_resources


def en_datums_generated_given_resources(new_en_datums):
    en_datums_generated = []
    for i in range(len(new_en_datums)):
        datum_gen = db.reg.datum_gen_given_resource(new_en_datums[i]['resource']) 
        en_datums_generated.append(next(datum_gen))
    return en_datums_generated


#filepath = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/an_2f218f"
fPath = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/"
an_filenames = [an for an in os.listdir(fPath) if an.startswith('an')]
en_filenames = [en for en in os.listdir(fPath) if en.startswith('en')]


an_resources, an_datums = gen_an_resources_datums(fPath, an_filenames)
en_resources, en_datums = gen_en_resources_datums(fPath, en_filenames)

new_an_resources, new_an_datums = register_an_resources_datums(an_resources, an_datums)
new_en_resources, new_en_datums = register_en_resources_datums(en_resources, en_datums)

db.reg.register_handler("PIZZABOX_AN_FILE_TXT", PizzaBoxAnHandler)
db.reg.register_handler("PIZZABOX_EN_FILE_TXT", PizzaBoxEnHandler)


registered_an_resources = register_an_resources_given_datum_id(new_an_resources, new_an_datums)
an_datums_generated = an_datums_generated_given_resources(new_an_datums)

registered_en_resources = register_en_resources_given_datum_id(new_en_resources, new_en_datums)
en_datums_generated = en_datums_generated_given_resources(new_en_datums)

def user_filechoice(filenames):
    print("Which file would you like to look at?")
    for i in range(len(filenames)): print(str(i + 1)+". " + filenames[i])
    filechoice = int(input("Choose number: "))
    return filechoice - 1

an_filechoice = user_filechoice(an_filenames)
an_fh = PizzaBoxAnHandler(resource_path=registered_an_resources[an_filechoice]['resource_path'],
    **registered_an_resources[an_filechoice]['resource_kwargs'])
an_datum = an_datums_generated[an_filechoice]
an_data = an_fh(**an_datum['datum_kwargs'])


en_filechoice = user_filechoice(en_filenames)
en_fh = PizzaBoxEnHandler(resource_path=registered_en_resources[en_filechoice]['resource_path'],
    **registered_en_resources[en_filechoice]['resource_kwargs'])
en_datum = en_datums_generated[en_filechoice]
en_data = en_fh(**en_datum['datum_kwargs'])
