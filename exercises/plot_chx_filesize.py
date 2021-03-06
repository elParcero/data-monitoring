'''
Author: Jorge Diaz Jr
This program does plotting for CHX file usage
for both the scraped method and the databroker method
'''
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

import os
import pandas as pd
import datetime

def create_df(file_path):
    df = pd.read_csv(file_path, sep=' ')
    #df.index = pd.to_datetime(df.pop('timestamp'))
    df.index = pd.to_datetime(df.pop('date'))
    return df

def plot_filesize(df):
    labels = range(24)
    plt.ion()
    df = df.resample('D').sum()
    df = df.cumsum()
    #df = df.groupby(df.index.hour).sum()

    fig, ax = plt.subplots()
    col_name = df.columns.values[0]

    plt.bar(df.index, df[col_name] * 1e-9, width=2.75)#, align='edge')
    fig.autofmt_xdate(bottom=0.20, rotation=57, ha='right')
    ax.set_title('CHX File Usage (scraped)')
    ax.set_xlabel('Daily Cumulative Sum')
    ax.set_ylabel('File Usage (GB)')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d %y'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.set_xlim(xmin=datetime.datetime(2015,10,1),xmax=datetime.datetime(2018,6,8))
    ax.set_ylim(ymax=65000)
    #plt.xticks(labels)
    #plt.ylim(ymax=2000)
    plt.show()


# databroker method 
# file_path = '/home/jdiaz/projects/data-monitoring/exercises/chx_filesize.dat'

# scraped method
file_path = '/home/jdiaz/beamline_plotting/beamline_file_sizes_v2/xf11id-ws2_file_sizes/data.dat'

df = create_df(file_path)
plot_filesize(df)
