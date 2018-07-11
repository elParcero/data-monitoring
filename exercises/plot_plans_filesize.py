# Author: Jorge Diaz
# plotting file usage for different plans from CHX beamline
import os

import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

years = mdates.YearLocator()    # every year
months = mdates.MonthLocator()  # every month
yearsFmt = mdates.DateFormatter('%Y')
plt.ion()


def readin_files(file_path, dat_files):
    data = dict()
    for file in dat_files:
        df = pd.read_csv(file_path + '/' + file, sep=" ")
        df.index = pd.to_datetime(df.pop('timestamp'))
        data[file.replace('.dat', '').replace('_FILESIZE_V2', '').upper()] = df
    return data


file_path = '/home/jdiaz/projects/data-monitoring/exercises/plan_plots'
dat_files = [dat_file for dat_file in os.listdir(file_path)
             if dat_file.endswith('.dat')]

# files read in and saved as dataframes
data = readin_files(file_path, dat_files)

# for key, dat in data.items():
#    dat.to_csv('{}_V2.dat'.format(key), sep=" ")


def plot_usage(data):
    plt.clf()

    for key, dat in data.items():
        col_name = dat.columns.values[0]
        dat = dat.resample('W').sum()
        dat = dat.cumsum()
        fig, ax = plt.subplots()
        plt.bar(dat.index, dat[col_name] * 1e-9, width=7,
                label=col_name.upper(), color='navy')
        ax.set_xlabel('Time (daily)')
        ax.set_ylabel('Usage (GB)')
        ax.set_title(key)
        fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
        ax.xaxis.set_minor_locator(mdates.DayLocator())
        plt.show()
        plt.legend(loc=2)
        plt.savefig(key + '.png')


plot_usage(data)
