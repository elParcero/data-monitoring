# Author: Jorge Diaz
# plotting file usage for different plans from CHX beamline
import os

import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

plt.ion()


def readin_files(file_path, dat_files):
    data = dict()
    for file in dat_files:
        df = pd.read_csv(file_path + '/' + file, sep=" ")
        df.index = pd.to_datetime(df.pop('timestamp'))
        data[file.replace('.dat', '').replace('_FILESIZE_V2', '').upper()] = df
    return data


def plot_usage(data, resample='W'):
    plt.clf()

    for key, dat in data.items():
        col_name = dat.columns.values[0]
        dat = dat.resample(resample).sum()
        dat = dat.cumsum()
        print(dat)
        fig, ax = plt.subplots()
#        plt.bar(dat.index, dat[col_name] * 1e-9, width=7,
#               label=col_name.upper(), color='navy')
        plt.plot(dat.index, dat[col_name] * 1e-9,
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
#        plt.savefig(key + '.png')


def line_plot_hourly_sum(hourly_sum):
    labels = range(24)
    for key, data in hourly_sum.items():
        col_name = data.columns.values[0]
        fig, ax = plt.subplots()
        plt.plot(data.index, data[col_name] * 1e-9,
                 label=col_name.upper(), color='navy')
        ax.set_xlabel('Time (hourly)')
        ax.set_ylabel('Usage (GB)')
        ax.set_title('{} : HOURLY SUM'.format(key))
        ax.xaxis.set_major_locator(ticker.FixedLocator(labels))
        plt.show()
        plt.legend(loc=2)
        plt.savefig('{}_line_hourly.png'.format(key))


def bar_plot_hourly_sum(hourly_sum):
    labels = range(24)
    for key, data in hourly_sum.items():
        col_name = data.columns.values[0]
        fig, ax = plt.subplots()
        plt.bar(data.index, data[col_name] * 1e-9, width=0.5,
                label=col_name.upper(), color='navy')
        ax.set_xlabel('Time (hourly)')
        ax.set_ylabel('Usage (GB)')
        ax.set_title('{} : HOURLY SUM'.format(key))
        ax.xaxis.set_major_locator(ticker.FixedLocator(labels))
        plt.show()
        plt.legend(loc=2)
        plt.savefig('{}_bar_hourly.png'.format(key))


def line_plot_average_sum(average_sum):
    labels = range(24)
    for key, data in average_sum.items():
        col_name = data.columns.values[0]
        fig, ax = plt.subplots()
        plt.plot(data.index, data[col_name] * 1e-9,
                 label=col_name.upper(), color='navy')
        ax.set_xlabel('Time (hourly)')
        ax.set_ylabel('Usage (GB)')
        ax.set_title('{} : Average per Hour'.format(key) )
        ax.xaxis.set_major_locator(ticker.FixedLocator(labels))
        plt.show()
        plt.legend(loc=2)
        #plt.savefig('{}_line_hourly_mean.png'.format(key))


def resample_sum(data, sampling='H'):
    hourly_sum = dict()
    for key, dat in data.items():
        dat = dat.resample(sampling).sum()
        dat = dat.groupby(dat.index.hour).sum()
        hourly_sum[key] = dat
    return hourly_sum


def average_sum(data, sampling='H'):
    average_sum = dict()
    for key, dat in data.items():
        dat = dat.resample(sampling).sum()
        dat = dat.groupby(dat.index.hour).mean()
        average_sum[key] = dat
    return average_sum
file_path = '/home/jdiaz/projects/data-monitoring/exercises/plan_plots'
dat_files = [dat_file for dat_file in os.listdir(file_path)
             if dat_file.endswith('.dat')]


# files read in and saved as dataframes
data = readin_files(file_path, dat_files)
# for key, dat in data.items():
#    dat.to_csv('{}_V2.dat'.format(key), sep=" ")

hourly_sum = resample_sum(data)
'''
#for key, data in hourly_sum.items():
#    print(key)
#    data.to_csv('{}_hourly_sum.dat'.format(key.lower()), sep=' ')

average_sum = average_sum(data)
for key, data in average_sum.items():
    print(key)
    data.to_csv('{}_hourly_average.dat'.format(key.lower()), sep=' ')
'''
# plot_usage(data)
line_plot_hourly_sum(hourly_sum)
bar_plot_hourly_sum(hourly_sum)
# line_plot_average_sum(average_sum)
