'''
Author: Jorge Diaz Jr
'''
from databroker import Broker
import pandas as pd

import os

import datetime
import time
from time import mktime

from eiger_io.fs_handler import EigerHandler
from databroker.assets.handlers import AreaDetectorTiffHandler

from pymongo.errors import CursorNotFound
from collections import defaultdict

def find_keys(db, since, until):
    '''
        This function searches for keys that are stored via filestore in a
        database, and gathers the SPEC id's from them.
    '''
    FILESTORE_KEY = "FILESTORE:"
    keys_dict = defaultdict(lambda : int (0))
    used_resources = set()

    files = []

    hdrs = db(since=since, until=until)
    hdrs = iter(hdrs)
    while True:
        try:
            hdr = next(hdrs)  
            for stream_name in hdr.stream_names:
                events = hdr.events(stream_name=stream_name)
                events = iter(events)
                while True:
                    try:
                        event = next(events)
                        if "filled" in event:
                            # there are keys that may not be filled
                            for key, val in event['filled'].items():
                                if not val:
                                    # get the datum
                                    if key in event['data']:
                                        datum_id = event['data'][key]
                                        try:
                                            resource = db.reg.resource_given_datum_id(datum_id)
                                        except:
                                            print('No datum found for resource: {}'.format(datum_id))
                                        resource_id = resource['uid']
                                        if resource_id in used_resources:
                                            continue
                                        else:
                                            used_resources.add(resource_id)
                                            datum_gen = db.reg.datum_gen_given_resource(resource)
                                            try:
                                                datum_kwargs_list = [datum['datum_kwargs'] for datum in datum_gen]
                                            except TypeError:
                                                print('type error for resource: {}'.format(resource))
                                                continue
                                            try:
                                                fh = db.reg.get_spec_handler(resource_id)
                                            except OSError:
                                                print('OS error for resource: {}'.format(resource))
                                            file_lists = fh.get_file_list(datum_kwargs_list)
                                            file_sizes = get_file_size(file_lists)
                                            keys_dict[key] = keys_dict[key] + file_sizes
                                            print('{} : {}'.format(key, file_sizes))
                    except StopIteration:
                        break
                    except KeyError:
                        print('key error')
                        continue
        except CursorNotFound:
            print('CursorNotFound = {}'.format(hdr))
            curr_time = hdr.start['time']+1
            tstruct = time.strptime(time.ctime(curr_time), "%a %b %d %H:%M:%S %Y")
            new_time = time.strftime("%Y-%m-%d %H:%M:%S", tstruct)
            hdrs = iter(db(since=since, until=new_time))
            print("Restarting up to {new_time}".format(new_time=new_time))
        except StopIteration:
            break
    return keys_dict


def get_file_size(file_list):
    sizes = []
    for file in file_list:
        if os.path.isfile(file):
            sizes.append(os.path.getsize(file))
    return sum(sizes)


def readin_file(file_path):
    chx_keys = set()
    df = pd.read_csv(file_path, sep=' ')
    df.index = df.pop('detector')
    #for det in df['detector']:
    #    chx_keys.add(det)
    return list(chx_keys), df

def plot_det_filesize(df):
    plt.ion()
    plt.clf()

    fig, ax = plt.subplots()
    
    col_name = list(df.columns.values)[0]

    plt.bar(df.index,  df[col_name] * 1e-9, label = 'CHX Detectors')
    #plt.plot(df.index, df[col_name] * 1e-9, label = 'CHX detectors')
    fig.autofmt_xdate(bottom=0.55, rotation=57, ha='right')
    ax.set_xlabel('Detectors')
    ax.set_ylabel('File Usage (GB)')
    ax.set_title('CHX Detectors')
    plt.show()
    plt.legend(loc=1)


file_path = '/home/jdiaz/src/data-monitoring/exercises/chx_detectors.dat'
# chx_keys, _ = readin_file(file_path)

f_path = '/home/jdiaz/src/data-monitoring/exercises/chx_detectors_filesize.dat'
#_, f_size = readin_file(f_path)
#plot_det_filesize(f_size)


db = Broker.named("chx")
db.reg.register_handler("AD_EIGER", EigerHandler)
db.reg.register_handler("AD_EIGER2", EigerHandler)
db.reg.register_handler("AD_EIGER_SLICE", EigerHandler)
db.reg.register_handler("AD_TIFF", AreaDetectorTiffHandler)


#hdrs = db(since="2015-01-01", until="2018-12-31")
since="2015-01-01"
until="2018-12-31"
#hdrs = [db['82dc7677-ed65-4ef7-a3a2-db3c72b12ea7']]
keys_dict = find_keys(db, since=since, until=until)

'''
df = pd.DataFrame.from_dict(keys_dict, orient='index')
df.index.name = 'detector'
df.columns = ['file_size_usage']

#plot_det_filesize(df)
#df.to_csv('chx_detectors_filesize.dat', sep=' ')
'''
