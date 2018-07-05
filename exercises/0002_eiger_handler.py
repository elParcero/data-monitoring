from databroker import Broker
import pandas as pd
import os

import datetime
import time
from time import mktime

from eiger_io.fs_handler import EigerHandler
from databroker.assets.handlers import AreaDetectorTiffHandler

def file_sizes(hdrs, db):

    unique_resources = set()
    time_size = dict()
    file_sizes = list()
    FILESTORE_KEY = "FILESTORE:"
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
                                    datum_id = event['data'][key]
                                    resource = (db.reg.resource_given_datum_id(datum_id))
                                    resource_id = resource['uid']
                                    if resource_id in unique_resources:
                                        continue
                                    else:
                                        unique_resources.add(resource_id)
                                    datum_gen = db.reg.datum_gen_given_resource(resource_id)
                                    datum_kwargs = [datum['datum_kwargs'] for datum in datum_gen]
                                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(event['time'])))
                                    timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                                    timestamp = datetime.datetime.fromtimestamp(mktime(timestamp))
                                    # get the file handler using this
                                    fh = db.reg.get_spec_handler(resource_id)
                                    print(fh)
                                    file_sizes = sum(fh.get_file_sizes(datum_kwargs))
                                    print(file_sizes)
                                    time_size[timestamp] = file_sizes
                except StopIteration:
                    break
#               except KeyError:
#                   continue
    end_time = time.time()
    total_time = end_time - start_time
    print(total_time)
    return time_size

def get_file_size(file_list):
    sizes = []
    for file in file_list:
        sizes.append(os.path.getsize(file))
    return sum(sizes)

db = Broker.named("chx")
db.reg.register_handler("AD_EIGER", EigerHandler)
db.reg.register_handler("AD_EIGER2", EigerHandler)
db.reg.register_handler("AD_TIFF", AreaDetectorTiffHandler)

hdrs = iter(db(start_time="2016-08-19", stop_time="2016-08-22", plan_name='count'))

# first two are bad, so I'm skipping them
hdr = next(hdrs)
hdr = next(hdrs)

# good one
# hdr = next(hdrs)
file_sizes = file_sizes(hdrs, db)

# make dataframe
df = pd.DataFrame.from_dict(file_sizes, orient='index')
df.columns = ['file_size']
df.index.name = 'timestamp'

df.to_csv('chx_file_sizes.dat', sep=" ")

resource_path = '/data/chx/tiff'
resource_kwargs = {'template': '%s%s_%6.6d.tiff', 
                   'filename': '6ef23c84-d8d5-4157-a0ee', 
                   'frame_per_point': 1}
datum_kwargs = {'point_number' : 0}

fh = AreaDetectorTiffHandler(resource_path, **resource_kwargs)

data = fh(**datum_kwargs)

file_list = fh.get_file_list([datum_kwargs])
file_size = get_file_size(file_list)
