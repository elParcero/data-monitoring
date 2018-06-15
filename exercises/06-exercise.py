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
		for chunk in pd.read_csv(resource_path, chunksize=chunk_size, header=None):
			self.chunks_of_data.append(chunk)	
		pass

	def __call__(self, chunk_num):
		'''
		returns specified chunk number/index from list of all chunks created
		'''
		result = self.chunks_of_data[chunk_num]
		return result

	def get_chunks(self):
		'''
		returns the number of chunks for specific file
		'''
		num_of_chunks = len(self.chunks_of_data)
		return num_of_chunks


file_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/"
files_only = next(os.walk(file_path))[2] 
an_files_only = [file for file in files_only if file.startswith("an_")]	

# user will enter file want to work with
# user will get to enter what chunk number desired
# dataframe is returned 

def file_choice():
	'''
	user gets to select which file they want to work with
	returns number for file choice
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
	returns chunk choice
	'''
	print('There are {0} chunks in {1}.'.format( myreader.get_chunks(),  file))
	return int(input("Which chunk number will you want, choose between 0 - {}:\nChunk choice: ".format(myreader.get_chunks() - 1)))

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
