''' 
  Author: Jorge Diaz Jr
- Exercise 3
- Saving header data in a dictionary
- Ex: if line starts with '#' and line is
- '# Year: 2018' --> save like so --> hdr_data['Year'] = '2018'
- Storing column names in a list
- Storing data in a list(becomes list of lists)
- Creating pandas DataFrame
'''

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

hdr_data = {}
column_names = []
key_value = []
info_data = []
file_name = "/home/jdiaz/projects/data-monitoring/data/xas_data/xas_test_328.txt"

#opens file and reads each line, extracting key and value pair if line starts with '#'
with open(file_name) as file:
	for line in file:
		if line.startswith("#"):
			if len(line) > 1:
				key_value = line.split(":", 1)
				if len(key_value) == 2:
					key = key_value[0].replace("#","").strip()
					value = key_value[1].replace("\n","").strip()
					hdr_data[key] = value
				elif len(key_value) == 1:
					column_names = line.replace("#","").strip().split()
					continue
		if not line.startswith("#"):
			info_data.append(line.strip().split())	

#printing dictionary and list to make sure values are in place
print("Header Data")
print(hdr_data)
print("\nColumn Names")
print(column_names)

#creating a pandas dataframe from the data in the text file
df = pd.DataFrame(info_data)
df.columns = column_names
print("\nData Frame")
print(df)

# columns in df object are of Series type, following lines convert it and i0 columns to ndarray with float type values
it_data = np.array(df.it, dtype="float")
i0_data = np.array(df.i0, dtype="float")
energy_data = np.array(df.energy, dtype="float")

#plotting
plt.ion()
plt.figure(0)
plt.clf()
plt.plot(np.log(it_data/i0_data), color='#be0119')  #scarlet color
plt.xlabel('Scan Number')
plt.ylabel('log(it) / log(i0)')
plt.title("XAS_DATA")
plt.grid(True)
plt.show()

plt.figure(1)
plt.clf()
plt.plot(energy_data, np.log(it_data/i0_data), color='#be0119')  #scarlet color
plt.xlabel('energy (keV)')
plt.ylabel('log(it) / log(i0)')
plt.title("XAS_DATA")
plt.grid(True)
plt.show()