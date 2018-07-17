
from databroker import Broker
import pandas as pd
import os

import datetime
import time
from time import mktime

from eiger_io.fs_handler import EigerHandler
from databroker.assets.handlers import AreaDetectorTiffHandler

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker


def file_sizes(hdrs, db, detector):
    unique_resources = set()
    time_size = dict()
    # FILESTORE_KEY = "FILESTORE:"
    start_time = time.time()
    timestamp = 0.0

    for hdr in hdrs:
        for stream_name in hdr.stream_names:
            events = hdr.events(stream_name=stream_name)
            events = iter(events)
            while True:
                try:
                    event = next(events)
                    if "filled" in event:
                        # there are keys that may not be filled
                        for key, val in event['filled'].items():
                            # if this is true, then we have a datum
                                if not val:
                                    # get the datum
                                    if key in event['data']:
                                        # checking to see if desired detector matches key
                                        if key == detector:
                                            print('{} = {}'.format(key, detector))
                                            datum_id = event['data'][key]
                                            resource = (db.reg.resource_given_datum_id(datum_id))
                                            resource_id = resource['uid']
                                            if resource_id in unique_resources:
                                                continue
                                            else:
                                                unique_resources.add(resource_id)
                                            datum_gen = db.reg.datum_gen_given_resource(resource_id)
                                            try:
                                                datum_kwargs = [datum['datum_kwargs'] for datum in datum_gen]
                                            except TypeError:
                                                print('TypeError ... ignore ...')
                                            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(event['time'])))
                                            timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                                            timestamp = datetime.datetime.fromtimestamp(mktime(timestamp))
                                            # get the file handler using this
                                            fh = db.reg.get_spec_handler(resource_id)
                                            print(fh)
                                            try:
                                                file_list = fh.get_file_list(datum_kwargs)
                                                file_size = get_file_size(file_list)
                                            except KeyError:
                                                file_size = 0
                                            print(file_size)
                                            time_size[timestamp] = file_size
                except StopIteration:
                    break
                except KeyError:
                    print('key error' * 5)
                    continue
    end_time = time.time()
    total_time = end_time - start_time
    print(total_time)
    return time_size


def get_file_size(file_list):
    sizes = []
    for file in file_list:
        if os.path.isfile(file):
            sizes.append(os.path.getsize(file))
    return sum(sizes)


def create_dfs(fPath, files):
    dfs = []
    for file in files:
        df = pd.read_csv(fPath + '/' + file, sep=',')
        df.index = pd.to_datetime(df.pop('timestamp'))
        dfs.append(df)
    return dfs


def plot_file_usage(dfs):
    plt.ion()
    plt.clf()

    for df in dfs:
        col_name = df.columns.values[0]
        df = df.resample('H').sum()
        df = df.cumsum()

        fig, ax = plt.subplots()
        plt.bar(df.index, df[col_name] * 1e-9, width=0.6, color='navy')
        fig.autofmt_xdate(bottom=0.2, rotation=57, ha='right')
        ax.set_title(col_name.replace('(fileusage)', '').upper())
        ax.set_xlabel('Hourly')
        ax.set_ylabel('File Usage (GB)')
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %y'))
        ax.xaxis.set_minor_locator(mdates.DayLocator())
        plt.show()
        # plt.savefig('{}.png'.format(col_name.replace('(fileusage)', '').replace(':', '_')))


def plot_hourly_sum(dfs):
    plt.ion()
    plt.clf()
    labels = range(24)

    for df in dfs:
        col_name = df.columns.values[0]
        df = df.resample('H').sum()
        df = df.groupby(df.index.hour).sum()

        fig, ax = plt.subplots()
        plt.bar(df.index, df[col_name] * 1e-9, width=0.6, color='navy')
        ax.set_title(col_name.replace('(fileusage)', '').upper())
        ax.set_xlabel('Hourly')
        ax.set_ylabel('File Usage (GB)')
        ax.xaxis.set_major_locator(ticker.FixedLocator(labels))
        plt.show()
        # plt.savefig('bar_{}.png'.format(col_name.replace('(fileusage)', '').replace(':', '_')))


db = Broker.named("chx")
db.reg.register_handler("AD_EIGER", EigerHandler)
db.reg.register_handler("AD_EIGER2", EigerHandler)
db.reg.register_handler("AD_EIGER_SLICE", EigerHandler)
db.reg.register_handler("AD_TIFF", AreaDetectorTiffHandler)

plan_names = ['count', 'scan', 'rel_scan']
detector_names = ['eiger1m_single_image', 'eiger4m_single_image', 'xray_eye2_image']

'''
This commented code, retrieved the file size for
plan name and detector, we saved it to a csv file using ipython
we will leave this here as proof of how we got the file sizes

det_size = dict()
for plan in plan_names:
    for detector in detector_names:
        hdrs = iter(db(since='2015-01-01', until='2018-12-31', plan_name=plan))
        det_size['{}:{}'.format(plan, detector)] = file_sizes(hdrs, db, detector)
'''

file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
files = [file for file in os.listdir(file_path) if file.endswith('.dat')]


dfs = create_dfs(file_path, files)
plot_file_usage(dfs)
plot_hourly_sum(dfs)
