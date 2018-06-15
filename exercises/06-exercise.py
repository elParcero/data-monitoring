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

class ANREADER:
	def __init__(self, resource_path, chunk_size = 1024):
		self.chunks_of_data = []
		for chunk in pd.read_csv(resource_path, chunksize=chunk_size, header=None):
			self.chunks_of_data.append(chunk)
			
		pass

	def __call__(self, chunk_num):
		result = self.chunks_of_data[chunk_num]
		return result

	def get_chunks(self):
		num_of_chunks = len(self.chunks_of_data)
		return num_of_chunks


file_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/"
files_only = next(os.walk(file_path))[2] 
an_files_only = [file for file in files_only if file.startswith("an_")]	

# user will enter file want to work with
# user will get to enter what chunk number desired
# dataframe is returned 

def file_choice():
	print('Which file would you like to look at?')
	index = 1
	for file in an_files_only:
		print(str(index) + ':' + file)
		index += 1
	return int(input("Enter number: "))

def chunk_choice(myreader, file):
	print('There are {0} chunks in {1}.'.format( myreader.get_chunks(),  file))
	return int(input("Which chunk number will you want, choose between 0 - {}:\nChunk choice: ".format(myreader.get_chunks() - 1)))

file_choice = file_choice() - 1

myreader = ANREADER(file_path + an_files_only[file_choice - 1], chunk_size=1024)

chunk_choice = chunk_choice(myreader, an_files_only[file_choice])

newreader = myreader(chunk_choice)
print(newreader)
