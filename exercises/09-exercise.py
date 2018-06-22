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
    date_format = "%Y-%m-%d"

    for i in range(len(file_names)):
        with open(file_path + file_names[i]) as file:
            for line in file:
                if index == 0:
                    index += 1
                    continue
                else:
                    key_val = line.split(" ")
                    datetime_obj = datetime.strptime(key_val[0].strip(), date_format)
                    date_size[datetime_obj] = float(key_val[1]) 
        file_size.append(date_size)
        date_size = {}
        index = 0
    return file_size


def create_df(date_size):
    '''
    receives a dictionary as argument, 
    creates a dataframe for each file
    indices are datetime objects, file_size column contains file size
    for each corresponding date
    returns a list of dataframes (each file gets a dataframe)
    '''
    dfs = []
    for index in range(len(date_size)):
        df = pd.DataFrame(pd.Series(date_size[index]), index= pd.DatetimeIndex(date_size[index].keys()), columns = ['file_size'])
        dfs.append(df)
    return dfs


def plot_usage_versus_day(dfs, file_names):
    '''
    receives the list of dataframes and file_names as arguments
    this function will plot usage (GB) versus day
    this function will also save the images as .png files
    '''
    for i in range(len(dfs)):
        fig, ax = plt.subplots()
        ax.bar( dfs[i].index, dfs[i].file_size *1e-9, width=1)
        ax.set_xlabel('Time')
        ax.set_ylabel('Usage (GB)')
        ax.set_title(file_names[i].replace(".txt", "").upper())
        # format the ticks
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)
        fig.savefig(file_names[i].replace(".txt", "").upper() + "_" + str(i) +"_daily.png")


def plot_usage_versus_week(dfs, file_names):
    '''
    receives the list of dataframes and file_names as arguments
    this function will plot usage (GB) versus week
    this function will also save the images as .png files
    '''
    for i in range(len(dfs)):
        newdf = dfs[i].resample('W').sum()
        fig, ax = plt.subplots()
        ax.bar( newdf.index, newdf.file_size *1e-9, width=7)
        ax.set_xlabel('Time')
        ax.set_ylabel('Usage (GB)')
        ax.set_title(file_names[i].replace(".txt", "").upper())
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)
        fig.savefig(file_names[i].replace(".txt", "").upper() + "_" + str(i) +"_weekly.png")

def plot_usage_versus_month(dfs, file_names):
    '''
    receives the list of dataframes and file_names as arguments
    this function will plot usage (GB) versus month
    this function will also save the images as .png files
    '''
    for i in range(len(dfs)):
        newdf = dfs[i].resample('M').sum()
        fig, ax = plt.subplots()
        ax.bar( newdf.index, newdf.file_size *1e-9, width=30)
        ax.set_xlabel('Time')
        ax.set_ylabel('Usage (GB)')
        ax.set_title(file_names[i].replace(".txt", "").upper())
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)
        fig.savefig(file_names[i].replace(".txt", "").upper() + "_" + str(i) +"_monthly.png")


file_path = '/home/jdiaz/projects/data-monitoring/data/file_sizes/'
file_names = os.listdir(file_path)
print(file_names)

#list that holds dictionary for each file read in >> key = date, value = file size
date_size = readin_file(file_path, file_names)

#now create a dataframe object for each file which will be held in a list (dfs)
dfs = create_df(date_size)

plot_usage_versus_day(dfs, file_names)
plot_usage_versus_week(dfs, file_names)
plot_usage_versus_month(dfs, file_names)
