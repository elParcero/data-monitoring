import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

import pandas as pd
import os
import numpy as np

from skbeam.core.accumulators.histogram import Histogram
from collections import defaultdict

from pandas.tools.plotting import autocorrelation_plot

def create_dfs(file_path, files):
    dfs = defaultdict(dict)

    for file in files:
        df = pd.read_csv(file_path + '/' + file, sep=',')
        df.index = pd.to_datetime(df.pop('timestamp'))
        col_name = df.columns.values[0]
        name,det = col_name.split(':',1)
        det = det.replace('(fileusage)','')
        print(name + ' '+det)
        dfs[name][det] = df
    return dfs

def plot_matrix(dfs):
    plt.ion()
    i = 1
    fig, axs = plt.subplots(2,2, sharex=True)
    plans = ['count','rel_scan']#,'scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image']#, 'xray_eye2_image']

    for i, plan in enumerate(plans):
        for j, detector in enumerate(detectors):
            ax = axs[i,j]
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                df = df.resample('D').sum()
                df =df.cumsum()

                #print(df.cumsum())
                col_name = df.columns[0]
                ax.bar(df.index, (df[col_name] * 1e-9), width=5, color='firebrick')
                ax.xaxis.set_major_locator(mdates.YearLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.MonthLocator())
                if(i == 0 and j == 0 or i == 1 and j == 0):
                    ax.set_ylabel(col_name.split(':')[0] + ' - fileusage(GB)')
                if(i == 0 and j == 0 or i == 0 and j == 1):
                    ax.set_title(col_name.replace('(fileusage)','').split(':')[1])
    plt.suptitle('CHX Plans + Detectors')
    plt.title('Daily Cumulative Sum', position=(-.14,-0.30))

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

def plot_histogram(dfs):
    min_size = 0
    max_size = 5e8 # 10 GB here
    Nbins = 10000
    labels = [num for num in range(11000) if num % 2000 == 0]
    minor_labels = [num for num in range(11000) if num % 1000 == 0]
    minor_2_labels = [num for num in range(11000) if num % 500 == 0]
    #y_minor_labels = [num for num in range()]
    x_labels = [i for i in range(10)]
    plt.ion()
    fig, axs = plt.subplots(2,2)
    plans = ['count','rel_scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image']

    for i, plan in enumerate(plans):
        for j, detector in enumerate(detectors):
            ax = axs[i,j]
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                h = Histogram((Nbins, min_size, max_size))
                col_name = df.columns[0]
                col = np.array(df[col_name])
                h.fill(col)
                ax.semilogy(h.centers[0][1:] * 1e-9, h.values[1:], color='darkgreen')
                ax.set_ylim(ymin=1.1)
                ax.set_xlim(xmin=1e-7)
                if i == 1 and j == 1:
                    ax.set_ylim(ymax=10)
                if(i == 0 and j == 0 or i == 1 and j == 0):
                    ax.set_ylabel(col_name.split(':')[0] + ' - # of occurences')
                if(i == 0 and j == 0 or i == 0 and j == 1):
                    ax.set_title(col_name.replace('(fileusage)','').split(':')[1])
                
    plt.suptitle('CHX Plans + Detectors')
    plt.title('Histogram Semi-Log Plot', position=(-.14,-0.30))



file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]
dfs = create_dfs(file_path, files)

#plot_matrix(dfs)
#plot_dets(dfs)
plot_histogram(dfs)


