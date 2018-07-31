'''
Author: Jorge Diaz Jr
Plots histogram for CHX plans
'''
import os
from skbeam.core.accumulators.histogram import Histogram
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

plt.ion()

def readin_files(file_path, files):
    file_usage = dict()
    for file in files:
        df = pd.read_csv(file_path + '/' + file, sep=' ')
        df = df.drop('timestamp', axis=1)
        col_name = df.columns.values[0]
        arr = np.array(df[col_name])
        file_usage[col_name.replace('(','_').replace(')','')] = arr
    return file_usage

def histogram_plot(file_usage, log_yaxis=False):
    min_size = 0
    max_size = 1e9 # 10 GB here
    Nbins = 10000

    for plan in file_usage:
        fig, ax = plt.subplots()
        h = Histogram((Nbins, min_size, max_size))
        h.fill(file_usage[plan])
        ax.set_xlabel('FILE USAGE')
        ax.set_ylabel('FREQUENCY')
        ax.set_title('CHX | Plan:Detector')
        if log_yaxis:
            ax.set_yscale('log')
        plt.plot(h.centers[0] *1e-6, h.values, label=plan)
        plt.legend()
        plt.savefig('hist_{}'.format(plan))


file_path = '/home/jdiaz/projects/data-monitoring/exercises/plan_plots'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]

file_usage = readin_files(file_path, files)

histogram_plot(file_usage, True)
