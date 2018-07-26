import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

import os
import pandas as pd

def create_df(file_path):
    df = pd.read_csv(file_path, sep=' ')
    df.index = pd.to_datetime(df.pop('timestamp'))
    #df.index = pd.to_datetime(df.pop('date'))
    return df

def plot_filesize(df):
    labels = range(24)
    plt.ion()
    df = df.resample('D').sum()
    #df = df.cumsum()
    #df = df.groupby(df.index.hour).sum()

    fig, ax = plt.subplots()
    col_name = df.columns.values[0]

    plt.bar(df.index, df[col_name] * 1e-9, width=2.75)#, align='edge')
    fig.autofmt_xdate(bottom=0.20, rotation=57, ha='right')
    ax.set_title('CHX File Usage (databroker)')
    ax.set_xlabel('Daily Sum')
    ax.set_ylabel('File Usage (GB)')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    #plt.xticks(labels)
    #plt.ylim(ymax=2000)
    plt.show()

file_path = '/home/jdiaz/projects/data-monitoring/exercises/chx_filesize.dat'
#file_path = '/home/jdiaz/beamline_plotting/beamline_file_sizes_v2/xf11id-ws2_file_sizes/data.dat'
df = create_df(file_path)
plot_filesize(df)
