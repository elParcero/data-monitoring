'''
	Author: Jorge Diaz
	Exercise 9
'''

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import os

years = mdates.YearLocator()   # every year
months = mdates.MonthLocator()  # every month
yearsFmt = mdates.DateFormatter('%Y')
plt.ion()

def readin_file(file_path, file_names):
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
	date_size = dict()
	for i in range(len(file_names)):
		with open(file_path + file_names[i]) as file:
			for line in file:
				if index == 0:
					index += 1
					continue
				else:
					key_val = line.split(" ")
					#date.append(key_val[0].strip())
					#file_size.append(float(key_val[1].strip()))
					date_size[key_val[0].strip()] = key_val[1].strip()
		file_size.append(date_size)
		date_size = {}
	return file_size

def create_df(date, file_size):
	'''
	receives two lists, date and file_size
	creates a dataframe such that date is the index and file_size the column
	'''
	df = pd.DataFrame(file_size, index = pd.DatetimeIndex(date), columns = ['file_size'])
	return df

def plot_usage_versus_day(df):
	fig, ax = plt.subplots()
	ax.bar( df.index, df.file_size *1e-9, width=1)
	ax.set_xlabel('Time')
	ax.set_ylabel('Usage (GB)')
	ax.set_title('CHX')

    # format the ticks
	ax.xaxis.set_major_locator(years)
	ax.xaxis.set_major_formatter(yearsFmt)
	ax.xaxis.set_minor_locator(months)

	#fig.savefig('chx_daily_usage.png')


def plot_usage_versus_week(df):
	newdf = df.resample('W').sum()
	print(newdf)
	fig, ax = plt.subplots()
	ax.bar( newdf.index, newdf.file_size *1e-9, width=7)
	ax.set_xlabel('Time')
	ax.set_ylabel('Usage (GB)')
	ax.set_title('CHX')

	ax.xaxis.set_major_locator(years)
	ax.xaxis.set_major_formatter(yearsFmt)
	ax.xaxis.set_minor_locator(months)
	
	#fig.savefig('chx_weekly_usage.png')

def plot_usage_versus_month(df):
	newdf = df.resample('M').sum()
	print(newdf)
	fig, ax = plt.subplots()
	ax.bar( newdf.index, newdf.file_size *1e-9, width=30)
	ax.set_xlabel('Time')
	ax.set_ylabel('Usage (GB)')
	ax.set_title('CHX')
	
	ax.xaxis.set_major_locator(years)
	ax.xaxis.set_major_formatter(yearsFmt)
	ax.xaxis.set_minor_locator(months)

	#fig.savefig('chx_monthly_usage.png')

file_path = '/home/jdiaz/projects/data-monitoring/data/file_sizes/'
file_names = os.listdir(file_path)
print(file_names)

#list that holds dictionary for each file read in >> key = date, value = file size
date_size= readin_file(file_path, file_names)

'''
df = create_df(date, file_size)
print(df)

plot_usage_versus_day(df)
plot_usage_versus_week(df)
plot_usage_versus_month(df)
'''