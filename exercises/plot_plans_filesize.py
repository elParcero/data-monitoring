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

file_path = '/home/jdiaz/beamline_plans_filesize'
dat_files = [dat_file for dat_file in os.listdir(file_path) if dat_file.endswith('.dat')]

data = readin_files(file_path, dat_files)

