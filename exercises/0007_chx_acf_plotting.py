import os
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf
from scipy.signal import correlate

plt.ion()

def create_dfs(file_path, files):
    dfs = []
    for file in files:
        df = pd.read_csv(file_path + '/' + file, sep=' ')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs


def plot_autocorrelation(dfs):
    for df in dfs:
        fig, ax = plt.subplots()
        col_name = df.columns.values[0]
        df = df.resample('H').sum()
        df = df[col_name].values.astype(float)
        cc = np.correlate(df, df, 'full')
        norm = np.ones_like(df)
        ccn = np.correlate(norm, norm, 'full')
        ax.set_xlabel('Hours')
        ax.set_ylabel('Correlation')
        ax.set_title(col_name.replace(':','_').upper())
        plt.plot(np.fft.fftshift(cc/ccn))
        plt.xlim(xmax=len(df))
        plt.show()
        print(col_name.replace(':','_').replace('(file_size)',''))
        #plt.savefig('chx_v2_{}'.format(col_name.replace(':','_').replace('(file_size)','')))


file_path = '/home/jdiaz/projects/data-monitoring/exercises/plan_plots'
#file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files =[file for file in os.listdir(file_path) if file.endswith('.dat')]

dfs = create_dfs(file_path, files)
plot_autocorrelation(dfs)
