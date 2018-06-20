'''
Author: Jorge Diaz
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
   	returns a dataframe object where the df is a specified
   	chunk of data
	'''
	def __init__(self, resource_path, chunk_size = 1024):
		'''
		adds the chunks of data to a list
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
		returns specified chunk number/index from list of all chunks created
		'''
		result = self.chunks_of_data[chunk_num]
		return result


	def __len__(self):
		'''
		returns the number of chunks for specific file
		'''
		return len(self.chunks_of_data)


def get_resource(resource_uid, filepath, filename):
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
	datum = {'datum_id': datum_uid,
 	'datum_kwargs': {'chunk_num': 2},
 	'resource': resource_uid
 	}
	return datum


def create_resource_datum(filenames):
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

# you would read it something like this
# fh is an object from the ANREADER class
# data uses the object created to invoke the __call__ method
fh = ANREADER(resources[0]['resource_path'], **resources[0]['resource_kwargs'])
data = fh(**datums[0]['datum_kwargs'])
