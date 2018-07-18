import os
import pandas as pd

import matplotlib.pyplot as plt


def create_dfs(file_path, files):
    dfs
    for file in files:
        df = pd.read_csv(file_path + '/' + file, sep='')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs

file_path = '/home/jdiaz/projects/data-monitoring/exercises/plan_plots'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]

