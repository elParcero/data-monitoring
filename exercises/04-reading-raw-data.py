'''
  Author: Jorge Diaz Jr
  Exercise 4
  Pt1. Reading files into an array
'''
import numpy as np 
import os
import pandas as pd

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
raw_files = [key for key in files_only if not key.endswith(".txt")]

# we will now read in all files into an array
# and store that array in a list of files readin
files_read_in = []
index = 0
for file in raw_files:
	readin_file = np.loadtxt(directory_string + "/" + raw_files[index])
	files_read_in.append(readin_file)
	index += 1

# creating an ndarray
# each index of this array is an array
file_in_dir = np.asarray(files_read_in)

for index in range(len(file_in_dir)):
	print("(Rows, Columns) - "+ str(file_in_dir[index].shape))


outer_index = 0
inner_index = 0
time = 0.0
times = []
temp_total_time = []
time2totaltime = []

# adding the sum of the time_in_sec AND time_in_nanosec of each ROW to a list
# once the list reaches the last index, the list is stored in another list that
# will hold the sum of time, from each row, of x file
for file in an_files_only:
	read_an_file = np.loadtxt(directory_string + "/" + an_files_only[outer_index])
	while inner_index < read_an_file.shape[0]:
		if inner_index != len(read_an_file):
			time_in_sec = read_an_file[inner_index][0]
			time_in_nanosec = read_an_file[inner_index][1]
			temp_total_time.append(time_in_sec + time_in_nanosec*1e-9)
			inner_index += 1
	time2totaltime.append(temp_total_time)
	temp_total_time = []
			
	inner_index = 0
	outer_index += 1

#we sum up all the elements(times) in individual array and store in a list 
sum_of_time = []
for time in time2totaltime:
	sum_of_time.append(np.sum(time))

#convert list to ndarray
sum_of_time = np.asarray(sum_of_time)

#print the sum of list
for i in range(len(sum_of_time)):
	print('Sum of total time in {0} = {1}s'.format(an_files_only[i], sum_of_time[i]))

#will read in each file, store contents in pandas dataframe, and store in dict where data[filename] = dataframe
data = dict()
index = 0

for file in an_files_only:
	df = pd.read_csv((directory_string + "/" + an_files_only[index]) , delimiter=" ", names = ['time (s)', 'time (ns)', 'index', 'counts'], header = None)
	data[file] = df
	index += 1

print("\n")
df['total time (s)'] = df['time (s)'] + 1e-9*df['time (ns)']
print(df)
