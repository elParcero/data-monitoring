# Author: Jorge Diaz
# plotting file usage for different plans from CHX beamline
import os
from datetime import datetime

import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from cycler import cycler

years = mdates.YearLocator()    # every year
months = mdates.MonthLocator()  # every month
yearsFmt = mdates.DateFormatter('%Y')
plt.ion()

def dateparse (time_in_secs):
    return datetime.datetime.fromtimestamp(float(time_in_secs))

def readin_files(file_path, dat_files):
    #data = []
    data = dict()
    for file in dat_files:
        df = pd.read_csv(file_path + '/' + file, sep=" ")
        df.index = pd.to_datetime(df['timestamp'])
        del df['timestamp']
        data[file.replace('.dat', '').upper()] = df
    return data

file_path = '/Users/jdiaz/data-monitoring/plans_chx_plots'
dat_files = [dat_file for dat_file in os.listdir(file_path) if dat_file.endswith('.dat')]

# files read in and saved as dataframes
data = readin_files(file_path, dat_files)


def plot_usage_versus_day(data):
    plt.clf()
    keys = [k for k in data]

    for key in data:
        fig, ax = plt.subplots()
        plt.bar(data[key].index, data[key] * 1e-9, width=1, alpha=0.6,
                color='r', label=data[key].columns)
        ax.set_xlabel('Time (daily)')
        ax.set_ylabel('Usage (GB)')
        ax.set_title(key)
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.xaxis.set_minor_locator(months)
        plt.show()
        plt.legend(loc=2)


plot_usage_versus_day(data)







