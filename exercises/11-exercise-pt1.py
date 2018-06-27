'''
Author: Jorge Diaz
Reading in File Sizes and plotting statistics
'''

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import os
from os.path import basename

from bokeh.io import show, output_file
from bokeh.plotting import figure
from bokeh.models import Title


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
        with open(file_path + '/' +file_names[i]) as file:
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


file_path = '/home/jdiaz/projects/data-monitoring/data/xf23id-ws3_file_sizes'
file_names = os.listdir(file_path)
print(file_names)

#list that holds dictionary for each file read in >> key = date, value = file size
date_size = readin_file(file_path, file_names)

#now create a dataframe object for each file which will be held in a list (dfs)
dfs = create_df(date_size)


def plot_usage_versus_day(dfs, file_path, file_names):
    for i in range(len(dfs)):

        title_1 = basename(file_path).split('-',1)[0].upper()
        title_2 = file_names[i].replace(".dat", "").upper()
        output_file('test' + str(i+1) + '_d.html')
        p = figure(plot_width = 1000, plot_height = 700, x_axis_type='datetime', title= title_1 + ' : ' + title_2)

        x_vals = dfs[i].index.tolist()        # timestamp
        y_vals = list(dfs[i].iloc[:,0]* 1e-9) # file size 

        p.vbar(x = x_vals, top = y_vals, width = 1, color = "#e6550d")
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.add_layout(Title(text="Time (days)", align='center'), 'below')
        p.add_layout(Title(text='File Size', align='center'), 'left')
        show(p)


def plot_usage_versus_week(dfs, file_path, file_names):
    for i in range(len(dfs)):
        
        title_1 = basename(file_path).split('-',1)[0].upper()
        title_2 = file_names[i].replace(".dat", "").upper()
        output_file('test' + str(i+1) + '_w.html')
        newdf = dfs[i].resample('W').sum()
        p = figure(plot_width = 1000, plot_height = 700, x_axis_type='datetime', title= title_1 + ' : ' + title_2)


        x_vals = newdf.index.tolist()  #timestamp
        y_vals = list(newdf.iloc[:,0] * 1e-9) #usage

        p.vbar(x = x_vals, top = y_vals, width = 70, color = "#e6550d")
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.add_layout(Title(text="Time (weeks)", align='center'), 'below')
        p.add_layout(Title(text='File Size', align='center'), 'left')
        show(p)


def plot_usage_versus_month(dfs, file_path, file_names):
    for i in range(len(dfs)):
        
        title_1 = basename(file_path).split('-',1)[0].upper()
        title_2 = file_names[i].replace(".dat", "").upper()
        output_file('test' + str(i+1) + '_m.html')
        newdf = dfs[i].resample('M').sum()
        p = figure(plot_width = 1000, plot_height = 700, x_axis_type='datetime', title= title_1 + ' : ' + title_2)


        x_vals = newdf.index.tolist()  #timestamp
        y_vals = list(newdf.iloc[:,0] * 1e-9) #usage

        p.vbar(x = x_vals, top = y_vals, width = 30, color = "#e6550d")
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.add_layout(Title(text="Time (months)", align='center'), 'below')
        p.add_layout(Title(text='File Size', align='center'), 'left')
        show(p)


#plot_usage_versus_day(dfs, file_path, file_names)

plot_usage_versus_week(dfs, file_path, file_names)

#plot_usage_versus_month(dfs, file_path, file_names)