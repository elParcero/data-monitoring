''' 
Author: Jorge Diaz Jr
Exercise 3
'''
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

file_path = "/home/jdiaz/projects/data-monitoring/data/xas_data/xas_test_328.txt"

def get_header(file_path):
	'''
	Open file , reads file in, splits line apart if it starts with #, 
	one piece is the key, the other piece is the value
	e.g. # Year: 2018
	gets stored in a dictionary such that hdr_dict['Year'] = '2018'
	returns the dictionary hdr_data, and returns a list of all the lines
	such that the lines do NOT start with # symbol
	'''
	hdr_data = {}
	key_value = []
	info_data = []

	#opens file and reads each line, extracting key and value pair if line starts with '#'
	with open(file_path) as file:
		for line in file:
			if line.startswith("#"):
				if len(line) > 1:
					key_value = line.split(":", 1)
					if len(key_value) == 2:
						key = key_value[0].replace("#","").strip()
						value = key_value[1].replace("\n","").strip()
						hdr_data[key] = value
					elif len(key_value) == 1:
						continue	
			if not line.startswith("#"):
				info_data.append(line.strip().split())

	return hdr_data, info_data

def get_column_names(file_path):
	'''
	argument in function is file path
	this function reads the lines to extract
	column names and returns them as a list
	'''
	column_names = []
	with open(file_path) as file:
		for line in file:
			if line.startswith("#"):
				if len(line) > 1:
					key_value = line.split(":", 1)
					if len(key_value) == 1:
						column_names = line.replace("#","").strip().split()
						continue
	return column_names

def create_df(info_data, column_names):
	'''
	the list made up of lines is passed in as well as column names
	creates a dataframe object and returns it
	'''
	df = pd.DataFrame(info_data)
	df.columns = column_names
	print("\nData Frame")
	print(df)
	return df 

def plot_log_i0_it(i0_data, it_data):
	'''
	plotting log(i0_data / it_data)
	'''
	plt.figure(0)
	plt.clf()
	plt.plot(np.log(i0_data/it_data), color='#be0119')  #scarlet color
	plt.xlabel('Scan Number')
	plt.ylabel('log(i0) / log(it)')
	plt.title("XAS_DATA")
	plt.grid(True)
	plt.show()

def plot_versus_energy(i0_data, it_data, energy_data):
	'''
	plotting log(i0_data / it_data) vs energy where energy will be x-axis
	'''
	plt.figure(1)
	plt.clf()
	plt.plot(energy_data, np.log(i0_data/it_data), color='#be0119')  #scarlet color
	plt.xlabel('energy (keV)')
	plt.ylabel('log(i0) / log(it)')
	plt.title("XAS_DATA")
	plt.grid(True)
	plt.show()

#hdr_data is a dictionary | info_data is a list where each element is a line in the file
hdr_data, info_data = get_header(file_path)
# geting column names
column_names = get_column_names(file_path)

# creating a dataframe from the data in the text file
df = create_df(info_data, column_names)

#printing dictionary and list to make sure values are in place
print("Header Data")
print(hdr_data)
print("\nColumn Names")
print(column_names)

# columns in df object are of Series type, following lines convert 
# it and i0 and energy COLUMNS to ndarray with float type values
it_data = np.array(df.it, dtype="float")
i0_data = np.array(df.i0, dtype="float")
energy_data = np.array(df.energy, dtype="float")

plt.ion()

# plotting np.log(i0_data/it_data)
plot_log_i0_it(i0_data , it_data)

#plotting versus energy with log
plot_versus_energy(i0_data, it_data, energy_data)