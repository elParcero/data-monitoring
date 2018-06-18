'''
	Author: Jorge Diaz
	Exercise 9
'''

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

def readin_file(file_path):
	'''
	Takes in file path as argument
	Reads file and splits the two columns into two lists
	one list contains dates, the other column contains filesizes
	associated with the dates
	RETURNS: the two lists, date and file_size
	'''
	index = 0
	date = []
	file_size = []
	with open(file_path) as file:
		for line in file:
			if index == 0:
				index += 1
				continue
			else:
				key_val = line.split(" ")
				date.append(key_val[0].strip())
				file_size.append(float(key_val[1].strip()))
	return date, file_size

def create_df(date, file_size):
	'''
	receives two lists, date and file_size
	creates a dataframe such that date is the index and file_size the column
	'''
	df = pd.DataFrame(file_size, index = pd.DatetimeIndex(date), columns = ['file_size'])
	return df

def plot_usage_versus_day(df):
	plot_0 = df.plot(title = "Usage vs Days", linewidth=.75, grid = True)
	plot_0.set_xlabel("Days")
	plot_0.set_ylabel('Usage')
	plt.show()

def plot_usage_versus_week(df):
	newdf = df.resample('W').mean()
	plot_1 = newdf.plot(title='Usage vs Weeks', linewidth=.75, grid=True)
	plot_1.set_xlabel("Weeks")
	plot_1.set_ylabel('Usage')
	plt.show()
	return

def plot_usage_versus_month(df):
	newdf = df.resample('M').mean()
	plot_2 = newdf.plot.bar(title='Usage vs Weeks', linewidth=.75, grid=True)
	plot_2.set_xlabel("Weeks")
	plot_2.set_ylabel('Usage')
	plt.show()
	return

file_path = '/home/jdiaz/projects/data-monitoring/data/file_sizes/'
file_name = 'chx.txt'

date, file_size= readin_file(file_path + file_name)
df = create_df(date, file_size)
print(df)


plot_usage_versus_day(df)
plot_usage_versus_week(df)
plot_usage_versus_month(df)