'''
  Author: Jorge Diaz Jr
  Exercise 4
'''
import numpy as np 
import os
import pandas as pd

'''
Some helper functions. We don't have to worry about the details.
These are hardware level details.
Use these to apply to the 'counts' column for an files etc
'''
fc = 7.62939453125e-05
adc2counts = lambda x: ((int(x, 16) >> 8) - 0x40000) * fc \
        if (int(x, 16) >> 8) > 0x1FFFF else (int(x, 16) >> 8)*fc
enc2counts = lambda x: int(x) if int(x) <= 0 else -(int(x) ^ 0xffffff - 1)

def raw_read_in(raw_files):
	raw_files_read_in = []
	index = 0
	for file in raw_files:
		readin_file = np.loadtxt(directory_string + "/" + raw_files[index])
		raw_files_read_in.append(readin_file)
		index += 1
	return raw_files_read_in

def files_in_dir(raw_files_read_in):
	return np.asarray(raw_files_read_in)

def print_size_files(file_read_in_dir, raw_files):
	for index in range(len(file_read_in_dir)):
		print(str(raw_files[index]) + " - (Rows, Columns) - "+ str(file_read_in_dir[index].shape))

def total_time(an_files_only, directory_string):
	'''
	adding the sum of the time_in_sec AND time_in_nanosec of each ROW to a list
	once the list reaches the last index, the list is stored in another list that
	will hold the sum of time, from each row, of x file
	'''
	outer_index = 0
	inner_index = 0
	time = 0.0
	temp_total_time = []
	time2totaltime = []

	for file in an_files_only:
		read_an_file = np.loadtxt(directory_string + "/" + an_files_only[outer_index])
		while inner_index < read_an_file.shape[0]: # less than number of rows in file
			if inner_index != len(read_an_file): # if inner index != the length or num of elements of file
				time_in_sec = read_an_file[inner_index][0] #gets time in sec
				time_in_nanosec = read_an_file[inner_index][1]#gets time for nanosec
				temp_total_time.append(time_in_sec + time_in_nanosec*1e-9)#sums to total time
				inner_index += 1 #move to next row
		time2totaltime.append(temp_total_time)
		temp_total_time = []
			
		inner_index = 0
		outer_index += 1

	return time2totaltime

def sum_times(time2totaltime):
	sum_of_time = []
	for time in time2totaltime:
		sum_of_time.append(np.sum(time))

	return sum_of_time

def print_sum_of_time(sum_of_time):
	#print the sum of list
	for i in range(len(sum_of_time)):
		print('Sum of total time in {0} = {1}s'.format(an_files_only[i], sum_of_time[i]))
	print('\n')


def an_files_to_dict(an_files_only):
	index = 0
	an_data = dict()
	temp_df = []
	#temp_an_list = []
	for file in an_files_only:
		an_df = pd.read_csv((directory_string + "/" + an_files_only[index]) , delimiter=" ", names = ['time (s)', 'time (ns)', 'index', 'counts'], header = None)
		an_df['volts'] = an_df['counts'].apply(adc2counts)
		an_df['total time (s)'] = an_df['time (s)'] + 1e-9*an_df['time (ns)']
		an_data[file] = an_df

		temp_df.append(an_df)
		index += 1

	return an_data, temp_df

def new_an_dfs(an_df):
	new_an_df = []
	for df in an_df:
		new_df = df[['total time (s)' , 'counts']].copy()	
		new_df['counts'] = new_df['counts'].apply(adc2counts)
		new_an_df.append(new_df)
	return new_an_df

def en_files_to_dict(en_files_only):
	index = 0
	en_data = dict()
	temp_df = []
	for file in en_files_only:
		en_df = pd.read_csv(directory_string + "/" + en_files_only[index], delimiter=" ", names = ['time (s)', 'time (ns)', 'encoder', 'index', 'di'], header = None)
		en_df['total time (s)'] = en_df['time (s)'] + 1e-9 * en_df['time (ns)']
		encoder_to_energy = en_df['encoder'].apply(enc2counts)
		en_df['energy'] = encoder_to_energy
		en_data[file] = en_df
		temp_df.append(en_df)
		index += 1
	return en_data, temp_df

def dev_names(dev_names_path):
	dev_names = dict()
	with open(dev_names_path) as file:
		for line in file:
			if not line.startswith("#"):
				temp_split = line.split(":",1)
				if len(temp_split) > 1:
					dev_names[temp_split[0].strip()] = temp_split[1].strip()
	return dev_names

directory_string = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data"
dev_names_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/devnames.txt"

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

raw_files_read_in = raw_read_in(raw_files)

# creating an ndarray
# each index of this array is an array itself
# containing the data read in from EACH file
file_read_in_dir = files_in_dir(raw_files_read_in)
print_size_files(file_read_in_dir, raw_files)

#list that holds the sum of time in each row of each file
time2totaltime = total_time(an_files_only, directory_string)

#we sum up all the elements(times) in individual array and store in a list 
sum_of_time = sum_times(time2totaltime)

#convert list to ndarray
sum_of_time = np.asarray(sum_of_time)
print('\n')
print_sum_of_time(sum_of_time)

# creating dictionary such that keys = filename, values = pandas dataframe
an_files_dict, an_df = an_files_to_dict(an_files_only)

# list that holds dataframe for each an_file, will only have two cols: total time (s) and counts 
new_an_df = new_an_dfs(an_df)

# preprocessing en_files
# dictionary contains key=filename which maps to value=dataframe object
# en_df is a list of dataframes from en files read in
en_files_dict, en_df = en_files_to_dict(en_files_only)

def manipulate_en_df(en_df):
	en_df_new = []
	for df in en_df:
		new_en_df = df[['total time (s)' , 'energy']].copy()	
		en_df_new.append(new_en_df)
	return en_df_new

# new list that holds dataframes from en files with 'total time (s)' and 'energy' columns only
new_en_dfs = manipulate_en_df(en_df)

# creating a dictionary with key: device names , value: device quantity
devnames = dev_names(dev_names_path)
