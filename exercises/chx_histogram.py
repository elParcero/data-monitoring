'''
Author: Jorge Diaz Jr
Plots histogram for CHX plans + detectors
'''
import os
from skbeam.core.accumulators.histogram import Histogram
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

plt.ion()
plt.clf()

def create_dfs(file_path, files):
    dfs = []
    for file in files:
        df = pd.read_csv(file_path +'/'+ file, sep=',')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs


def histogram_plot(dfs):
    min_size = 0
    max_size = 1e9 # 10 GB here
    Nbins = 10000
    fig, ax = plt.subplots()
    x_range = np.linspace(0,1e-6, 10000)
    for df in dfs:
        h = Histogram((Nbins, min_size, max_size))
        col_name = df.columns.values[0]
        col = np.array(df[col_name])
        h.fill(col)
        ax.set_xlabel('FILE USAGE')
        ax.set_ylabel('FREQUENCY')
        ax.set_title('CHX | Plan:Detector')
        plt.plot(h.centers[0] *1e-6, h.values, label=col_name)
    plt.legend()



file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]

dfs = create_dfs(file_path, files)
histogram_plot(dfs)

# at each iteration for a file size fill histogram
#h.fill(file_size)

#to plot histogram values
#plt.plot(h.centers, h.values)
# the bin centers 