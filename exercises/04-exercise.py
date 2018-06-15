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
	'''
	the raw files to read in
	Returns a list of raw files read in as arrays
	'''
	raw_files_read_in = []
	index = 0
	for file in raw_files:
		readin_file = np.loadtxt(directory_string + raw_files[index])
		raw_files_read_in.append(readin_file)
		index += 1
	return raw_files_read_in

def files_in_dir(raw_files_read_in):
	'''
	returns the list of raw files read in, that is, the data for 
	each file is stored in an index of a ndarray
	returns a numpy array
	'''
	return np.asarray(raw_files_read_in)

def print_size_files(file_read_in_dir, raw_files):
	'''
	prints how many rows and how many columns there are for 
	each raw file read in
	'''
	for index in range(len(file_read_in_dir)):
		print(str(raw_files[index]) + " - (Rows, Columns) - "+ str(file_read_in_dir[index].shape))

def print_sum_of_time(sum_of_time):
	'''
	prints the total sum of the totaltime column for each file
	'''
	for i in range(len(sum_of_time)):
		print('Sum of total time in {0} = {1}s'.format(an_files_only[i], sum_of_time[i]))
	print('\n')


def read_an_file(file_name):
	'''
	creating a data frame for specified file name
	and returns a dataframe that will get stored in a dictionary
	'''
	an_df = pd.read_csv((directory_string + file_name) , delimiter=" ", names = ['time (s)', 'time (ns)', 'index', 'counts'], header = None)
	an_df['volts'] = an_df['counts'].apply(adc2counts)
	an_df['total time (s)'] = an_df['time (s)'] + 1e-9*an_df['time (ns)']
	return an_df


def an_files_to_dict(an_files_only):
	'''
	creates a dictionary in which key = filename and the value = dataframe
	associated with that specific file
	'''
	an_data = dict()
	temp_df = []
	for file_name in an_files_only:
		an_data[file_name] = read_an_file(file_name)
		temp_df.append(an_data[file_name])
		
	return an_data, temp_df

def new_an_dfs(an_df):
	'''
	returns a new dataframe for an_files with only
	two columns --> col0 = total time (s) col1 = counts
	'''
	new_an_df = []
	for df in an_df:
		new_df = df[['total time (s)' , 'counts']].copy()	
		new_df['counts'] = new_df['counts'].apply(adc2counts)
		new_an_df.append(new_df)
	return new_an_df

def read_en_file(file_name):
	'''
	creating a data frame for specified file name
	and returns a dataframe that will get stored in a dictionary
	'''
	en_df = pd.read_csv(directory_string + file_name, delimiter=" ", names = ['time (s)', 'time (ns)', 'encoder', 'index', 'di'], header = None)
	en_df['total time (s)'] = en_df['time (s)'] + 1e-9 * en_df['time (ns)']
	encoder_to_energy = en_df['encoder'].apply(enc2counts)
	en_df['energy'] = encoder_to_energy
	return en_df

def en_files_to_dict(en_files_only):
	'''
	creates a dictionary in which key = filename and the value = dataframe
	associated with that specific file
	returns en_dictionary and dataframes(stored in list) for each file
	'''
	en_data = dict()
	temp_df = []
	for file_name in en_files_only:
		en_data[file_name] = read_en_file(file_name)
		temp_df.append(en_data[file_name])
		
	return en_data, temp_df

def new_en_dfs(en_df):
	'''
	creating a new dataframe with only two columns
	col0 = total time (s)- col1 = energy
	returns a list with the new dataframe from each en_file 
	'''
	en_df_new = []
	for df in en_df:
		new_en_df = df[['total time (s)' , 'energy']].copy()	
		en_df_new.append(new_en_df)
	return en_df_new

def dev_names(dev_names_path):
	'''
	reads in a text file and finds the device name along 
	with the energy 
	returns a dictionary with >> key = device name , value = energy
	'''
	dev_names = dict()
	with open(dev_names_path) as file:
		for line in file:
			if not line.startswith("#"):
				temp_split = line.split(":",1)
				if len(temp_split) > 1:
					dev_names[temp_split[0].strip()] = temp_split[1].strip()
	return dev_names

def file_names(file_names_path):
	'''
	reads in a text file and finds the file name along 
	with the device name 
	returns a dictionary with >> key = file name , value = device name
	'''
	file_names = dict()
	with open(file_names_path) as file:
		for line in file:
			if not line.startswith('#'):
				temp_split = line.split(':', 1)
				if len(temp_split) > 1:
					file_names[temp_split[0].strip()] = temp_split[1].strip()
	return file_names

def file_quantity(filenames, devnames):
	'''
	arguments are two dictionaries
	filesnames maps file name to device name
	devnames maps device name to device quantity
	returns a dictionary >> key = filename, value = device quantity associated with filename
	'''
	file_quantity = dict()
	for file in filenames:
		device_name = filenames[file]
		device_quantity = devnames[device_name] 
		file_quantity[file] = device_quantity
	return file_quantity
	
directory_string = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/"
dev_names_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/devnames.txt"
file_names_path = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/filenames.txt"
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

# creating dictionary >>> keys = filename, values = pandas dataframe
an_files_dict, an_df = an_files_to_dict(an_files_only)

# list that holds dataframe for each an_file, will only have two cols: total time (s) and counts 
new_an_df = new_an_dfs(an_df)

# preprocessing en_files
# dictionary contains key=filename which maps to value=dataframe object
# en_df is a list of dataframes from en files read in
en_files_dict, en_df = en_files_to_dict(en_files_only)

# new list that holds dataframes from en files with 'total time (s)' and 'energy' columns only
new_en_df = new_en_dfs(en_df)

# creating a dictionary with key: device names , value: device quantity
devnames = dev_names(dev_names_path)

# creating a dictionary with key: file names , value: device name
filenames = file_names(file_names_path)
	
# dictionary that stores file name as key and quantity as value
files_quantity = file_quantity(filenames, devnames)
