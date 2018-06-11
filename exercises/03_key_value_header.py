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
print(hdr_data)
print(column_names)


#creating a pandas dataframe from the data in the text file
df = pd.DataFrame(info_data)
df.columns = column_names
print(df)

#plotting
plt.figure(0)
plt.clf()

np.log(df.it/df.i0)
