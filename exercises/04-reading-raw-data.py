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

txt_files_only = [file for file in files_only if file.endswith(".txt")]
an_files_only = [file for file in files_only if file.startswith("an_")]	
en_files_only = [file for file in files_only if file.startswith("en_")]

#all files without .txt extension
files_only = [key for key in files_only if not key.endswith(".txt")]

files_read_in = []

# we will now read in all files into an array
index = 0
for file in files_only:
	read_in_file = np.loadtxt(directory_string + "/" + files_only[index])
	print(type(read_in_file))
	files_read_in.append(read_in_file)
	index += 1

#print(files_read_in)
# creating an 
file_in_dir = np.asarray(files_read_in)
print(type(files_read_in))

print("Shape of ndarray is: " + str(file_in_dir.shape)) 

for i in range(len(file_in_dir)):
	print(file_in_dir[i].shape)

outer_index = 0
inner_index = 0
time = 0.0
times = []
time2totaltime = []

for file in an_files_only:
	read_an_file = np.loadtxt(directory_string + "/" + an_files_only[outer_index])
	print(len(read_an_file))
	while inner_index < read_an_file.size:
		if inner_index != len(read_an_file):
			time_sec = read_an_file[inner_index][0]
			time_nanosec = read_an_file[inner_index][1]
			time2totaltime.append(time_sec + time_nanosec*1e-9)
			inner_index += 1
	inner_index = 0
	outer_index += 1

