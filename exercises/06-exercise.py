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
from Tkinter import *

class ANREADER:
	def __init__(self, resource_path, file_name, chunk_size = 1024):
		self.chunks_of_data = []
		self.chunks_in_file = dict()
		
		for chunk in pd.read_csv(resource_path, chunksize=chunk_size, header=None):
			self.chunks_of_data.append(chunk)
			self.chunks_in_file[file_name] = self.chunks_of_data
			
		pass

	def __call__(self, file_name, chunk_num):
		result = self.chunks_in_file[file_name]
		return result[chunk_num]

	def get_key(self):
		key = list(self.chunks_in_file.keys())
		return key[0]

	def get_chunks(self, file_name):
		num_of_chunks = len(self.chunks_in_file[file_name])
		return num_of_chunks

file_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/"
files_only = next(os.walk(file_path))[2] 
an_files_only = [file for file in files_only if file.startswith("an_")]	

# user will enter file want to work with
# user will get to enter what chunk number desired
# dataframe is returned
file_choice = input('Which file would you like to look at?\n{}\n'.format('	'.join(an_files_only)) ) 
myreader = ANREADER(file_path + file_choice, file_choice, chunk_size=1024)

print('There are {0} chunks in {1}.'.format( myreader.get_chunks(file_choice), myreader.get_key() ))
chunk_choice = int(input("Which chunk number will you want, choose between 0 - {}:\n".format(myreader.get_chunks(file_choice) - 1)))

newreader = myreader(file_choice.strip(), chunk_choice)

print(newreader)
