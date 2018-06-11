'''
  Author: Jorge Diaz Jr
  Exercise 4
  Pt1. Reading files into an array
'''
import numpy as np 
import os

directory_string = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data"

'''
going to store each file name in a list
index 2 indicates that we only want filenames and no directory path or directory names
'next' grabs the next value in iterator 
'''
files_only = next(os.walk(directory_string))[2] 

txt_files_only = []

for file in files_only:
	if file.endswith(".txt"):
		txt_files_only.append(file)
	
files_only = [key for key in files_only if not key.endswith(".txt")]

print(files_only)
print(txt_files_only)
num_of_files = len(files_only)

files_read_in = []

# we will now read in all files into an array
index = 0
for file in files_only:
	read_in_file = np.loadtxt(directory_string + "/" + files_only[index])
	files_read_in.append(read_in_file)
	index += 1
#print(files_read_in)
# creating an 
file_in_dir = np.asarray(files_read_in)
print(type(files_read_in))

print("Shape of ndarray is: " + str(file_in_dir.shape)) 

#print(files_read_in)
#print("Shape of ndarray (Rows, Columns): " + str(files_read_in.shape))