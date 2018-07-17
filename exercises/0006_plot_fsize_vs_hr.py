import os
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker


def create_dfs(fPath, files):
    dfs = []
    for file in files:
        df = pd.read_csv(fPath + '/' + file, sep=',')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs

def plot_file_usage(dfs):
    plt.ion()
    plt.clf()

    for df in dfs:
        col_name = df.columns.values[0]
        df = df.resample('H').sum()
        df = df.cumsum()

        fig, ax = plt.subplots()
        plt.bar(df.index, df[col_name] * 1e-9, width=0.6, color='navy')
        fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')
        ax.set_title(col_name.replace('(fileusage)','').upper())
        ax.set_xlabel('Hourly')
        ax.set_ylabel('File Usage (GB)')
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
        ax.xaxis.set_minor_locator(mdates.DayLocator())
        plt.show()
        plt.savefig('{}.png'.format(col_name.replace('(fileusage)', '').replace(':','_')))



file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]

dfs = create_dfs(file_path, files)
plot_file_usage(dfs)
