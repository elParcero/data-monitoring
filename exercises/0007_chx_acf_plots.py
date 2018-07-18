import os
import pandas as pd

import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf

def create_dfs(file_path, files):
    dfs = []
    for file in files:
        df = pd.read_csv(file_path + '/' + file, sep=',')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs


def plot_autocorrelation(dfs):
    plt.ion()
    plt.clf()
    for df in dfs:
        col_name = df.columns.values[0].replace('(fileusage)','')
        if len(df.index) > 50:
            plot_acf(df, lags=50, title=col_name.upper())
            plt.show()
            plt.savefig('acf_{}'.format(col_name))
        else:
            plot_acf(df, title=col_name.upper())
            plt.show()
            plt.savefig('acf_{}'.format(col_name.replace(':', '_')))


file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]

dfs = create_dfs(file_path, files)
plot_autocorrelation(dfs)

