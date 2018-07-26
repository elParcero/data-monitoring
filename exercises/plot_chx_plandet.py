import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

import pandas as pd
import os


def create_dfs(file_path, files):
    dfs = []
    for file in files:
        df = pd.read_csv(file_path + '/' + file, sep=',')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs

def plot_matrix(dfs):
    plt.ion()
    plt.clf()
    i = 1
    fig, ax = plt.subplots()
    for df in dfs:
        
        col_name = df.columns.values[0]
        fig.add_subplot(3,3,i)
        plt.bar(df.index, df[col_name] * 1e-9)
        fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
        ax.xaxis.set_minor_locator(mdates.DayLocator())
        i += 1
    plt.show()

    '''
    plt.subplot(337)
    plt.bar(dfs[0].index, df[df.columns.values[0]] * 1e-9)
    plt.subplot(338)
    plt.bar(dfs[0].index, df[df.columns.values[0]] * 1e-9)
    plt.subplot(339)
    plt.bar(dfs[0].index, df[df.columns.values[0]] * 1e-9)
    '''
    plt.show()

    #for df in dfs:

def plot_dets(dfs):
    plt.ion()

    for df in dfs:
        df = df.resample('D').sum()
        fig, ax = plt.subplots()
        col_name = df.columns.values[0]
        plt.bar(df.index, df[col_name] * 1e-9, width=2.0)
        fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
        ax.xaxis.set_minor_locator(mdates.DayLocator())
        ax.set_ylabel('File usage (GB)')
        ax.set_xlabel('Daily Sum')
        ax.set_title(col_name.replace('(fileusage)','').replace(':',': ').upper())
    plt.show()


file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]
dfs = create_dfs(file_path, files)

#plot_matrix(dfs)
plot_dets(dfs)