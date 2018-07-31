'''
this program was authored by Jorge Diaz

Function of this program is to read in files that
contain file usage information for the plans+detectors
from CHX beamline.

The goal is to be able to have
Matrix plots
    - cumulative sum plots
    - file rate plots
    - file rate semi log
    - histogram file usage plots
    - semi-log yaxis histogram file usage plots
& Normal versions of these

'''
from skbeam.core.accumulators.histogram import Histogram

import numpy as np
import pandas as pd
import os
from collections import defaultdict

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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


def mplot_cumulative(dfs):
    plt.ion()
    fig, axs = plt.subplots(2,2, sharex=True)
    plans = ['count','rel_scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image']

    for i, plan in enumerate(plans):
        for j, detector in enumerate(detectors):
            ax = axs[i,j]
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                df = df.resample('D').sum()
                df =df.cumsum()

                col_name = df.columns[0]
                ax.bar(df.index, (df[col_name] * 1e-9), width=5)
                ax.xaxis.set_major_locator(mdates.YearLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.MonthLocator())
                if(i == 0 and j == 0 or i == 1 and j == 0):
                    ax.set_ylabel(col_name.split(':')[0] + ' - fileusage(GB)')
                if(i == 0 and j == 0 or i == 0 and j == 1):
                    ax.set_title(col_name.replace('(fileusage)','').split(':')[1])
    plt.suptitle('CHX Plans + Detectors')
    plt.title('Daily Cumulative Sum', position=(-.14,-0.30))


def mplot_file_rate(dfs):
    plt.ion()
    fig, axs = plt.subplots(2,2, sharex=True)
    plans = ['count','rel_scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image']

    for i, plan in enumerate(plans):
        for j, detector in enumerate(detectors):
            ax = axs[i,j]
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                df = df.resample('D').sum()

                col_name = df.columns[0]
                ax.bar(df.index, (df[col_name] * 1e-9) / 24, width=5.5)
                ax.xaxis.set_major_locator(mdates.YearLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.MonthLocator())
                if(i == 0 and j == 0 or i == 1 and j == 0):
                    ax.set_ylabel(col_name.split(':')[0] + ' - file rate(GB/HR)')
                if(i == 0 and j == 0 or i == 0 and j == 1):
                    ax.set_title(col_name.replace('(fileusage)','').split(':')[1])
    plt.suptitle('CHX Plans + Detectors')
    plt.title('Hourly', position=(-.14,-0.30))


def mplot_file_rate_semilog(dfs):
    plt.ion()
    fig, axs = plt.subplots(2,2, sharex=True)
    plans = ['count','rel_scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image']

    for i, plan in enumerate(plans):
        for j, detector in enumerate(detectors):
            ax = axs[i,j]
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                df = df.resample('D').sum()

                col_name = df.columns[0]
                ax.bar(df.index, (df[col_name] * 1e-9) / 24)
                ax.set_yscale('log')
                ax.xaxis.set_major_locator(mdates.YearLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.MonthLocator())
                if(i == 0 and j == 0 or i == 1 and j == 0):
                    ax.set_ylabel(col_name.split(':')[0] + ' - file rate(GB/HR)')
                if(i == 0 and j == 0 or i == 0 and j == 1):
                    ax.set_title(col_name.replace('(fileusage)','').split(':')[1])
    plt.suptitle('CHX Plans + Detectors')
    plt.title('Hourly', position=(-.14,-0.30))


def mplot_histogram_fileusage(dfs):
    min_size = 0
    max_size = 1e10
    Nbins = 10000
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
                ax.plot(h.centers[0][1:] * 1e-9, h.values[1:])
                if(i == 0 and j == 0 or i == 1 and j == 0):
                    ax.set_ylabel(col_name.split(':')[0] + ' - # of occurences')
                if(i == 0 and j == 0 or i == 0 and j == 1):
                    ax.set_title(col_name.replace('(fileusage)','').split(':')[1])
    plt.suptitle('CHX Plans + Detectors')
    plt.title('Histogram Plot', position=(-.14,-0.30))


def mplot_histogram_fileusage_semilog(dfs):
    min_size = 0
    max_size = 5e8 
    Nbins = 10000
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
                ax.semilogy(h.centers[0][1:] * 1e-9, h.values[1:])
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


def plot_cumulative(dfs):
    plt.ion()
    plans = ['count','rel_scan', 'scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image', 'xray_eye2_image']
    for plan in (plans):
        for detector in (detectors):
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                fig, ax = plt.subplots()
                df = df.resample('D').sum()
                df =df.cumsum()
                col_name = df.columns[0]
                ax.bar(df.index, (df[col_name] * 1e-9), width=5)
                ax.xaxis.set_major_locator(mdates.MonthLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.DayLocator())
                ax.set_ylabel("File Usage (GB)")
                ax.set_xlabel("Daily Cumulative Sum")
                ax.set_title(col_name.replace(":",": ").replace("(fileusage)",""))
                ax.set_xlim(xmin="2016-07-01", xmax="2018-07-08")
                fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')


def plot_file_rates(dfs):
    plt.ion()
    plans = ['count','rel_scan', 'scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image', 'xray_eye2_image']
    for plan in plans:
        for detector in detectors:
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                fig, ax = plt.subplots()
                df = df.resample('D').sum()
                col_name = df.columns[0]
                ax.bar(df.index, (df[col_name] * 1e-9) /24, width=3)
                ax.xaxis.set_major_locator(mdates.MonthLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.DayLocator())
                ax.set_ylabel("File Rate (GB/HR)")
                ax.set_xlabel("Hourly Rate")
                ax.set_title(col_name.replace(":",": ").replace("(fileusage)",""))
                ax.set_xlim(xmin="2016-07-01", xmax="2018-07-08")
                fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')


def plot_file_rates_semilog(dfs):
    plt.ion()
    plans = ['count','rel_scan', 'scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image', 'xray_eye2_image']
    for plan in plans:
        for detector in detectors:
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                fig, ax = plt.subplots()
                df = df.resample('D').sum()
                col_name = df.columns[0]
                ax.bar(df.index, (df[col_name] * 1e-9) /24, width=2)
                ax.set_yscale('log')
                ax.xaxis.set_major_locator(mdates.MonthLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
                ax.xaxis.set_minor_locator(mdates.DayLocator())
                ax.set_ylabel("File Rate (GB/HR)")
                ax.set_xlabel("Hourly Rate")
                ax.set_title(col_name.replace(":",": ").replace("(fileusage)",""))
                ax.set_xlim(xmin="2016-07-01", xmax="2018-07-08")
                fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')


def plot_histogram_fileusage(dfs):
    min_size = 0
    max_size = 1e10
    Nbins = 10000
    plt.ion()
    plans = ['count','rel_scan', 'scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image', 'xray_eye2_image']

    for plan in plans:
        for detector in detectors:
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                fig, ax = plt.subplots()
                h = Histogram((Nbins, min_size, max_size))
                col_name = df.columns[0]
                col = np.array(df[col_name])
                h.fill(col)
                ax.plot(h.centers[0][1:] * 1e-9, h.values[1:])
                ax.set_ylabel('# of occurences')
                ax.set_xlabel('Histogram (gb)| Bins = 10000')
                ax.set_title(col_name.replace('(fileusage)','').split(':')[1])


def plot_histogram_fileusage_semilog(dfs):
    min_size = 0
    max_size = 1e10
    Nbins = 10000
    plt.ion()
    plans = ['count','rel_scan', 'scan']
    detectors = ['eiger1m_single_image', 'eiger4m_single_image', 'xray_eye2_image']

    for plan in plans:
        for detector in detectors:
            df = dfs.get(plan, dict()).get(detector, None)
            if df is not None:
                fig, ax = plt.subplots()
                h = Histogram((Nbins, min_size, max_size))
                col_name = df.columns[0]
                col = np.array(df[col_name])
                h.fill(col)
                ax.plot(h.centers[0][1:] * 1e-9, h.values[1:])
                ax.set_yscale('log')
                ax.set_ylabel('# of occurences')
                ax.set_xlabel('Histogram (gb)| Bins = 10000')
                ax.set_title(col_name.replace('(fileusage)','').split(':')[1])





file_path_plan_det = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path_plan_det) if file.endswith('.dat')]
dfs = create_dfs(file_path_plan_det, files)

#mplot_cumulative(dfs)
#mplot_file_rate(dfs)
#mplot_file_rate_semilog(dfs)
#mplot_histogram_fileusage(dfs)
#mplot_histogram_fileusage_semilog(dfs)

#plot_cumulative(dfs)
#plot_file_rates(dfs)
#plot_file_rates_semilog(dfs)
#plot_histogram_fileusage(dfs)
#plot_histogram_fileusage_semilog(dfs)
