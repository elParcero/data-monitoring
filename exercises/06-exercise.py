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
		#num_of_lines = len(open(resource_path).readlines())
		#num_of_chunks = int( num_of_lines / chunk_size )
		#num_of_remaining_chunk = num_of_lines % chunk_size
		chunks_of_data = []
		
		index = 0

		for chunk in pd.read_csv(resource_path, chunksize=chunk_size):
			print(chunk)
			print(index)
			index += 1
		
		pass

	def __call__(self, chunk_num):
		print(chunk_num)
		return result


file_path = directory_string = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data"


myreader = ANREADER(file_path + "/an_2f218f", chunk_size=1024)