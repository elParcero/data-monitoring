'''
  Author: Jorge Diaz Jr
  Exercise 4
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
raw_files_read_in = []
index = 0
for file in raw_files:
	readin_file = np.loadtxt(directory_string + "/" + raw_files[index])
	raw_files_read_in.append(readin_file)
	index += 1

# creating an ndarray
# each index of this array is an array
# containing the data from each file
file_in_dir = np.asarray(raw_files_read_in)

for index in range(len(file_in_dir)):
	print(str(raw_files[index]) + " - (Rows, Columns) - "+ str(file_in_dir[index].shape))


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
print('\n')

#print the sum of list
for i in range(len(sum_of_time)):
	print('Sum of total time in {0} = {1}s'.format(an_files_only[i], sum_of_time[i]))
print('\n')

#will read in each file, store contents in pandas dataframe, and store in dict where data[filename] = dataframe
an_data = dict()
index = 0

for file in an_files_only:
	an_df = pd.read_csv((directory_string + "/" + an_files_only[index]) , delimiter=" ", names = ['time (s)', 'time (ns)', 'index', 'counts'], header = None)
	an_df['total time (s)'] = an_df['time (s)'] + 1e-9*an_df['time (ns)']
	an_data[file] = an_df
	index += 1

# will convert counts 
fc = 7.62939453125e-05
adc2counts = lambda x: ((int(x,16) >> 8) - 0x40000) * fc \
				if (int(x,16) >> 8) > 0x1FFFF else (int(x,16) >> 8) * fc
				
an_df['counts'] = an_df['counts'].apply(adc2counts)
#print(an_df)

new_an_df = an_df[['total time (s)' , 'counts']].copy()
#print(new_an_df)

#preprocessing en file
en_files_read_in = []
index = 0
en_data = dict()

for file in en_files_only:
	en_df = pd.read_csv(directory_string + "/" + en_files_only[index], delimiter=" ", names = ['time (s)', 'time (ns)', 'encoder', 'index', 'di'], header = None)
	en_df['total time (s)'] = en_df['time (s)'] + 1e-9 * en_df['time (ns)']
	en_data[file] = en_df
	index += 1

def encoder2energy(encoder, pulses_per_deg=36000, offset=0.09833571377062045):
	return -12400 / (2 * 3.1356 * np.sin(np.deg2rad((encoder/pulses_per_deg) - float(offset))))

encoder_to_energy = encoder2energy(en_df['encoder'])
en_df['energy'] = encoder_to_energy
#print(en_df)

new_en_df = en_df[['total time (s)' , 'energy']].copy()
print(new_en_df)

# creating a dictionary with key: device names , value: device quantity
dev_names = dict()
dev_names_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/devnames.txt"
with open(dev_names_path) as file:
	for line in file:
		if not line.startswith("#"):
			temp_split = line.split(":",1)
			if len(temp_split) > 1:
				dev_names[temp_split[0].strip()] = temp_split[1].strip()

print(dev_names)

