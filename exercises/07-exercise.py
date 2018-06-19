'''
Author: Jorge Diaz
Exercise 7
File Handler
'''
import uuid
import pandas as pd
import os


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
		print(chunk_size)
		self.chunks_of_data = []
		for chunk in pd.read_csv(resource_path, chunksize=chunk_size, header=None):
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
 	'datum_kwargs': {'chunk_num': 0},
 	'resource': resource_uid}
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
fh = ANREADER(resources[0]['resource_path'], **resources[0]['resource_kwargs'])
data = fh(**datums[0]['datum_kwargs'])
