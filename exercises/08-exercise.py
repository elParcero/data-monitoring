'''
Author: Jorge Diaz
Exercise 8
'''
import uuid
import pandas as pd
from databroker.tests.utils import temp_config
from databroker import Broker
from databroker.assets.handlers_base import HandlerBase

# this will create a temporary databroker object with nothing in it
db = Broker.from_config(temp_config())

fc = 7.62939453125e-05
adc2counts = lambda x: ((int(x, 16) >> 8) - 0x40000) * fc \
        if (int(x, 16) >> 8) > 0x1FFFF else (int(x, 16) >> 8)*fc
enc2counts = lambda x: int(x) if int(x) <= 0 else -(int(x) ^ 0xffffff - 1)


class MyHandler(HandlerBase):
    def __init__(self, resource_path, chunk_size=1024):
        '''
        adds the chunks of data to a list
        '''
        self.chunks_of_data = []
        for chunk in pd.read_csv(resource_path, chunksize=chunk_size, 
        		names =['time (s)', 'time (ns)', 'index', 'counts'], 
        		delimiter = " ", header=None):
        	chunk['volts'] = chunk['counts'].apply(adc2counts)
        	chunk['total time (s)'] = chunk['time (s)'] + 1e-9*chunk['time (ns)']
        	chunk = chunk.drop(columns = ['time (s)', 'time (ns)', 'counts'])
        	chunk = chunk[['total time (s)', 'index', 'volts']]
        	self.chunks_of_data.append(chunk)

    def __call__(self, chunk_num):
        '''
        returns specified chunk number/index from list of all chunks created
        '''
        result = self.chunks_of_data[chunk_num]
        return result


def get_resource(resource_uid, filepath):
	resource = {'id': resource_uid,
 	'path_semantics': 'posix',
 	'resource_path': filepath,
 	'root': '/home/jdiaz',
 	'resource_kwargs': {},
 	'spec': 'PIZZABOX_AN_FILE_TXT',
 	'uid': resource_uid,
	}
	return resource


def get_datum(datum_uid, resource_uid):
	datum = {'datum_id': datum_uid,
 	'datum_kwargs': {'chunk_num' : 7},
 	'resource': resource_uid
 	}
	return datum


filepath = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/an_2f218f"

resource_uid = str(uuid.uuid4()) #create a unique id for resource
datum_uid = str(uuid.uuid4()) #create a unique id for datum
resource = get_resource(resource_uid, filepath) # retrieving dict for resource
datum = get_datum(datum_uid, resource['uid']) # retrieving dict for datum

# registering a handler according to a certain spec
new_resource = db.reg.insert_resource( "PIZZABOX_AN_FILE_TXT", resource_path = resource['resource_path'], \
	resource_kwargs = resource['resource_kwargs'], uid = resource['uid'])

new_datum = db.reg.insert_datum(resource = new_resource, datum_id = datum['datum_id'],\
 datum_kwargs = datum['datum_kwargs'])

db.reg.register_handler("PIZZABOX_AN_FILE_TXT", MyHandler)

resource = db.reg.resource_given_datum_id(new_datum['datum_id']) 

datum_gen = db.reg.datum_gen_given_resource(new_datum['resource']) 
datums = list(datum_gen) # converts generator object into list

# creating object of MyHandler class
fh = MyHandler(resource_path=resource['resource_path'],
	**resource['resource_kwargs'])
# we want the first datum 
datum = datums[0]
# this is the data retrieved from the specificed 'datum_kwargs'
data = fh(**datum['datum_kwargs'])