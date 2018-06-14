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

class ANREADER:
	def __init__(self, resource_path, chunk_size = 1024):
		global chunks_of_data
		chunks_of_data = []

		for chunk in pd.read_csv(resource_path, chunksize=chunk_size, header=None):
			chunks_of_data.append(chunk)
		
		print('There are {} chunks in this data set.'.format(len(chunks_of_data)))
		pass

	def __call__(self, chunk_num):
		result = chunks_of_data[chunk_num]
		return result


file_path = "/Users/jdiaz/data-monitoring/data/iss_sample_data"
file_name = "/an_2f218f"

myreader = ANREADER(file_path + file_name, chunk_size=1024)

# get the 10th chunk
# dataframe is returned
new = myreader(10)

