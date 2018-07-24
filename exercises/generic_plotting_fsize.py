import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

import os

import pandas as pd
import numpy as np

def readin_file(root_path, files):
    readin_files = dict()
    dfs = []
    for parent, child in files.items():
        for file in child:
            if parent == 'chx_dets_plans':
                df = pd.read_csv(root_path +'/' + parent+ '/' + file, sep=",")
                df.index = pd.to_datetime(df.pop('timestamp'))
                dfs.append(df)
                readin_files[parent] = dfs
            else:
                df = pd.read_csv(root_path +'/' + parent+ '/' + file, sep=" ")
                df.index = pd.to_datetime(df.pop('timestamp'))
                dfs.append(df)
                readin_files[parent] = dfs
        dfs = []
    return readin_files

def plot_w_resample(file_readin, resample='H', cumulative=False, regroup=''):
    plt.ion()
    plt.clf()
    labels = range(24)
    width = 0
    xlabel = ''
    if resample == 'H':
        width = 0.7
        xlabel = 'Hourly'
    elif resample == 'D':
        width = 1
        xlabel = 'Daily'
    elif resample == 'W':
        width = 7
        xlabel = 'Weekly'
    elif resample == 'M':
        width = 31
        xlabel = 'Monthly'

    for parent in file_readin:
        for df in file_readin[parent]:
            df = df.resample(resample).sum()
            if cumulative:
                df = df.cumsum()
            if regroup:
                if regroup == 'H':
                    df = df.groupby(df.index.hour).sum()
            col_name = df.columns.values[0]
            fig, ax = plt.subplots()
            if parent == 'chx_dets_plans':
                plt.bar(df.index, df[col_name] * 1e-9, width=width, color='blue')
                col_name = df.columns.values[0].replace('(fileusage)','').replace(':',': ')
            else:
                plt.bar(df.index, df[col_name] * 1e-9, width=width, color='red')
                col_name = df.columns.values[0].replace('(file_size)','')
            ax.set_title(col_name.upper())
            ax.set_xlabel(xlabel)
            ax.set_ylabel('File Usage (GB)')
            if regroup == 'H':
                ax.xaxis.set_major_locator(ticker.FixedLocator(labels))
                ax.xaxis.set_minor_locator(ticker.FixedLocator(labels))
            else:
                fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')
                ax.xaxis.set_major_locator(mdates.MonthLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.DayLocator())
            plt.show()

def plot_file_rate(file_readin):
    plt.ion()
    plt.clf()
    width = 0.7
    for parent in file_readin:
        for df in file_readin[parent]:
            df = df.resample('D').sum()
            col_name = df.columns.values[0]
            fig, ax = plt.subplots()
            if parent == 'chx_dets_plans':
                plt.bar(df.index, (df[col_name] * 1e-9) / 24, width=width, color='blue')
                col_name = df.columns.values[0].replace('(fileusage)','').replace(':',': ')
            else:
                plt.bar(df.index, (df[col_name] * 1e-9) / 24, width=width, color='red')
                col_name = df.columns.values[0].replace('(file_size)','')
            ax.set_title(col_name.upper())
            ax.set_xlabel('Hourly')
            ax.set_ylabel('File Rate (GB / HR)')
            fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
            ax.xaxis.set_minor_locator(mdates.DayLocator())
            plt.show()

root_path = '/home/jdiaz/projects/data-monitoring/exercises/file_sizes'
parent_dirs = os.listdir(root_path)
del parent_dirs[2]
del parent_dirs[0]

files = dict()
for directory in parent_dirs:
    child_files = os.listdir(root_path + '/' + directory)
    files[directory] = child_files

file_readin = readin_file(root_path, files)

#plot_w_resample(file_readin)
#plot_w_resample(file_readin, cumulative=True)
#plot_w_resample(file_readin, regroup='H')
#plot_w_resample(file_readin, resample='D')
#lot_w_resample(file_readin, resample='W')
#plot_w_resample(file_readin, resample='M')

#plot_file_rate(file_readin)

def plot_histogram(file_readin):

    for parent in file_readin:
        for df in file_readin[parent]:
            col_name = df.columns.values[0]
            fig, ax = plt.subplots()
            rng = df[col_name].max() - df[col_name].min()
            labels = [(i + 25) for i in range(int(rng))]
            hist, _ = np.histogram(df[col_name], bins=10)
            plt.hist(hist) 
            ax.set_title(col_name)
            ax.set_xlabel('File Usage (GB)')
            ax.set_ylabel('Frequency')
            ax.set_minor_locator(ticker.FixedLocator(labels))
            plt.show()
            break



plot_histogram(file_readin)