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

def file_sizes(db, since, until):
    '''
        This function searches for keys that are stored via filestore in a
        database, and gathers the SPEC id's from them.
    '''
    FILESTORE_KEY = "FILESTORE:"
    time_size = dict()
    used_resources = set()
    timestamp = 0.0

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
                                            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(event['time'])))
                                            timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                                            timestamp = datetime.datetime.fromtimestamp(mktime(timestamp))
                                            try:
                                                fh = db.reg.get_spec_handler(resource_id)
                                            except OSError:
                                                print('OS error for resource: {}'.format(resource))
                                            try:
                                                file_lists = fh.get_file_list(datum_kwargs_list)
                                                file_size = get_file_size(file_lists)
                                            except KeyError:
                                                print('key error for datum datum kwargs: {}'.format(datum_kwargs_list))
                                                file_size = 0.0
                                            time_size[timestamp] = file_size
                                            print(fh)
                                            print(file_size)
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
    return time_size


def get_file_size(file_list):
    sizes = []
    for file in file_list:
        if os.path.isfile(file):
            sizes.append(os.path.getsize(file))
    return sum(sizes)


db = Broker.named("chx")
db.reg.register_handler("AD_EIGER", EigerHandler)
db.reg.register_handler("AD_EIGER2", EigerHandler)
db.reg.register_handler("AD_EIGER_SLICE", EigerHandler)
db.reg.register_handler("AD_TIFF", AreaDetectorTiffHandler)

since="2015-01-01"
until="2018-12-31"
time_size = file_sizes(db, since=since, until=until)

'''
df = pd.DataFrame.from_dict(keys_dict, orient='index')
df.index.name = 'detector'
df.columns = ['file_size_usage']

#df.to_csv('chx_detectors_filesize.dat', sep=' ')
'''


