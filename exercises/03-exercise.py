''' 
Author: Jorge Diaz Jr
Exercise 3
'''

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

file_path = "/home/jdiaz/projects/data-monitoring/data/xas_data/xas_test_328.txt"

def get_header(file_path):
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
	df = pd.DataFrame(info_data)
	df.columns = column_names
	print("\nData Frame")
	print(df)
	return df 

def plot_log_it_i0(it_data, i0_data):
	plt.figure(0)
	plt.clf()
	plt.plot(np.log(it_data/i0_data), color='#be0119')  #scarlet color
	plt.xlabel('Scan Number')
	plt.ylabel('log(it) / log(i0)')
	plt.title("XAS_DATA")
	plt.grid(True)
	plt.show()

def plot_versus_energy(it_data, i0_data, energy_data):
	plt.figure(1)
	plt.clf()
	plt.plot(energy_data, np.log(it_data/i0_data), color='#be0119')  #scarlet color
	plt.xlabel('energy (keV)')
	plt.ylabel('log(it) / log(i0)')
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

# plotting np.log(it_data / i0_data)
plot_log_it_i0(it_data, i0_data)

#plotting versus energy
plot_versus_energy(it_data, i0_data, energy_data)