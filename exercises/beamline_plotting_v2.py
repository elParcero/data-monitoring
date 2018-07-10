'''
Author: Jorge Diaz Jr
'''
import os


from datetime import datetime
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from cycler import cycler

years = mdates.YearLocator()   # every year
months = mdates.MonthLocator()  # every month
yearsFmt = mdates.DateFormatter('%Y')
plt.ion()


def readin_file(files, file_path, parent_files):
    index = 0
    date_size = dict()
    oreo = dict()
    det_names = []
    date_format = "%Y-%m-%d"

    for i in range(len(files)):
        for j in range(len(files[parent_files[i]])):
            with open(file_path + parent_files[i] +
                      "/" + files[parent_files[i]][j]) as file:
                for line in file:
                    if index == 0:
                        index += 1
                        continue
                    else:
                        key_val = line.split(" ")
                        datetime_obj = datetime.strptime(key_val[0].strip(),
                                                         date_format)
                        oreo[datetime_obj] = float(key_val[1])
            key_name = parent_files[i].upper().split('-', 1)[0]
            det_name = files[parent_files[i]][j].replace(".dat", "").upper()
            det_names.append(det_name)
            date_size[key_name + ':' + det_name] = oreo
            oreo = {}
            index = 0

    return date_size, det_names


def create_df(file_size, det_names):
    dfs = dict()
    for key in file_size:
        df = pd.DataFrame.from_dict(file_size[key], orient='index')
        df.index.name = 'date'
        df.columns = [key.split(':', 1)[1]]
        main_key = key.split(':', 1)[0]
        if main_key not in dfs:
            dfs[main_key] = []
        dfs[main_key].append(df)

    return dfs


def plot_usage_versus_day(newdfs, file_names):
    plt.clf()
    color_cycle = cycler(color=['r', 'g', 'b'])
    beamline = [file.upper().split('-', 1)[0] for file in file_names]

    newdfs = [newdf.cumsum() for newdf in newdfs]
    for i in range(len(newdfs)):
        fig, ax = plt.subplots()
        for column, color in zip(newdfs[i].columns, color_cycle):
            df = newdfs[i][column]
            plt.bar(df.index, df * 1e-9, width=1, alpha=0.6,
                    color=color['color'], label=column)

            ax.set_xlabel('Time (daily)')
            ax.set_ylabel('Usage (GB)')
            ax.set_title(beamline[i].upper())
            ax.xaxis.set_major_locator(years)
            ax.xaxis.set_major_formatter(yearsFmt)
            ax.xaxis.set_minor_locator(months)
            plt.show()
            plt.legend(loc=2)
            plt.savefig(beamline[i] + "_daily_cumulative.png")


def plot_usage_versus_week(newdfs, file_names):
    plt.clf()
    color_cycle = cycler(color=['r', 'g', 'b'])
    beamline = [file.upper().split('-', 1)[0] for file in file_names]

    newdfs = [newdf.resample('W').sum() for newdf in newdfs]
    newdfs = [newdf.cumsum() for newdf in newdfs]

    for i in range(len(newdfs)):
        fig, ax = plt.subplots()
        print(i)
        for column, color in zip(newdfs[i].columns, color_cycle):
            df = newdfs[i][column]
            plt.bar(df.index, df * 1e-9, width=7, alpha=0.6,
                    color=color['color'], label=column)

            ax.set_xlabel('Time (weekly)')
            ax.set_ylabel('Usage (GB)')
            ax.set_title(beamline[i].upper())
            ax.xaxis.set_major_locator(years)
            ax.xaxis.set_major_formatter(yearsFmt)
            ax.xaxis.set_minor_locator(months)
            plt.show()
            plt.legend(loc=2)
#           plt.savefig(beamline[i] + "_weekly_cumulative.png")


def plot_usage_versus_month(newdfs, file_names):
    plt.clf()
    color_cycle = cycler(color=['r', 'g', 'b'])
    beamline = [file.upper().split('-', 1)[0] for file in file_names]

    newdfs = [newdf.resample('M').sum() for newdf in newdfs]
    newdfs = [newdf.cumsum() for newdf in newdfs]

    for i in range(len(newdfs)):
        fig, ax = plt.subplots()
        print(i)
        for column, color in zip(newdfs[i].columns, color_cycle):
            df = newdfs[i][column]
            plt.bar(df.index, df * 1e-9, width=31, alpha=0.6,
                    color=color['color'], label=column)
            ax.set_xlabel('Time (monthly)')
            ax.set_ylabel('Usage (GB)')
            ax.set_title(beamline[i].upper())
            ax.xaxis.set_major_locator(years)
            ax.xaxis.set_major_formatter(yearsFmt)
            ax.xaxis.set_minor_locator(months)
            plt.show()
            plt.legend(loc=2)
#           plt.savefig(beamline[i] + "_monthly_cumulative.png")


file_path = '/home/jdiaz/beamline_plotting/beamline_file_sizes_v2/'
parent_files = os.listdir(file_path)

files = dict()
for i in range(len(parent_files)):
    child_files = os.listdir(file_path + parent_files[i])
    files[parent_files[i]] = child_files

file_size, det_names = readin_file(files, file_path, parent_files)
dfs = create_df(file_size, det_names)
newdfs = []

for key in dfs:
    newdfs.append(pd.concat(dfs[key], axis=1))

plot_usage_versus_day(newdfs, list(files.keys()))
# plot_usage_versus_week(newdfs, list(files.keys()))
# plot_usage_versus_month(newdfs, list(files.keys()))
