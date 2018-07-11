from databroker import Broker
import pandas as pd

def find_keys(hdrs, db):
    '''
        This function searches for keys that are stored via filestore in a
        database, and gathers the SPEC id's from them.
    '''
    FILESTORE_KEY = "FILESTORE:"
    keys_dict = dict()
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
                            if key not in keys_dict and not val:
                                # get the datum
                                if key in event['data']:
                                    datum_id = event['data'][key]
                                    resource = db.reg.resource_given_datum_id(datum_id)
                                    keys_dict[key] = resource['spec']
                                    print(key)
                except StopIteration:
                    break
                except KeyError:
                    continue
    return keys_dict

db = Broker.named("chx")
hdrs = db(since="2015-01-01", until="2018-12-31")
'''
keys_dict = find_keys(hdrs, db)

df = pd.DataFrame.from_dict(keys_dict, orient='index')
df.index.name = 'detector'
df.columns = ['spec']

#df.to_csv('chx_detectors.dat', sep=' ')
'''

def readin_file(file_path):
    chx_keys = set()
    df = pd.read_csv(file_path, sep=' ')
    for det in df['detector']:
        chx_keys.add(det)
    return list(chx_keys)

file_path = '/home/jdiaz/projects/data-monitoring/exercises/chx_detectors.dat'
chx_keys = readin_file(file_path)
