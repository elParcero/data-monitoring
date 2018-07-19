import os
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

plt.ion()

def create_dfs(file_path, files):
    dfs = []
    for file in files:
        df = pd.read_csv(file_path + '/' + file, sep=',')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs

def plot_histogram(dfs):
    for df in dfs:
        fig, ax = plt.subplots()
        col_name = df.columns.values[0]
        x = df[col_name].values
        plt.hist(x, bins = 20)
        ax.set_title(col_name.replace('(fileusage)',''))
        plt.gca().set_xlim(x.min(), x.max())
        plt.show()
        plt.savefig('chx_{}'.format(col_name.replace('(fileusage)','').replace(':','_')))


#file_path = '/home/jdiaz/projects/data-monitoring/exercises/plan_plots'
file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]

dfs = create_dfs(file_path, files)

plot_histogram(dfs)
