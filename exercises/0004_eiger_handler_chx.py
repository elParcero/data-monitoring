
from databroker import Broker
import pandas as pd
import os

import datetime
import time
from time import mktime

from eiger_io.fs_handler import EigerHandler
from databroker.assets.handlers import AreaDetectorTiffHandler

def file_sizes(hdrs, db, detector):
    unique_resources = set()
    time_size = dict()
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
                                        print('level 3')
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


db = Broker.named("chx")
db.reg.register_handler("AD_EIGER", EigerHandler)
db.reg.register_handler("AD_EIGER2", EigerHandler)
db.reg.register_handler("AD_EIGER_SLICE", EigerHandler)
db.reg.register_handler("AD_TIFF", AreaDetectorTiffHandler)

plan_names = ['count', 'scan', 'rel_scan']
detector_names = ['eiger1m_single_image','eiger4m_single_image','xray_eye2_image']

det_size = dict()
#hdrs = iter(db(since='2018-04-27', until='2018-12-31', plan_name=plan_names[0]))
for plan in plan_names:
    for detector in detector_names:
        hdrs = iter(db(since='2015-01-01', until='2018-12-31', plan_name=plan))
        det_size['{}:{}'.format(plan, detector)] = file_sizes(hdrs, db, detector)

