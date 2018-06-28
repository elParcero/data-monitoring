''' 
  Author: Jorge Diaz Jr
- Exercise 3
- Storing column names in a list
'''

def get_column_names(file_name):
	column_names = []
	with open(file_name) as file:
		for line in file:
			if line.startswith("#"):
				if len(line) > 1:
					key_value = line.split(":", 1)
					if len(key_value) == 1:
						column_names = line.replace("#","").strip().split()
						continue
	return column_names

