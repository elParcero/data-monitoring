from databroker import Broker
import time

from databroker.assets.handlers import AreaDetectorTiffHandler

def find_keys(hdrs, db):
    '''
        This function searches for keys that are stored via filestore in a
        database, and gathers the SPEC id's from them.
        For example:
            from databroker import Broker
            db = Broker.named("chx")
            hdrs = db(since="2018-01-01", until="2018-04-01")
            find_keys(hdrs, db)
    '''
    FILESTORE_KEY = "FILESTORE:"
    keys_dict = dict()
    start_time = time.time()
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
                                    spec = resource['spec']
                                    if spec =='AD_TIFF':
                                        print("got resource {}".format(resource))
                                        tif_resource = resource
                                    keys_dict[key] = resource['spec']
                except StopIteration:
                    break
                except KeyError:
                    continue
    end_time = time.time()
    print(end_time - start_time)
    return keys_dict, tif_resource

db = Broker.named("chx")
hdrs = db(since="2018-01-01", until="2018-04-01")
keys_dict, tif_resource = find_keys(hdrs, db)

datum_gen = db.reg.datum_gen_given_resource(tif_resource)
datum = next(datum_gen)

# fpath, template, filename, frame_per_point=1
resource_path = tif_resource['resource_path']
resource_kwargs = tif_resource['resource_kwargs']
datum_kwargs = datum['datum_kwargs']

fh = AreaDetectorTiffHandler(resource_path, **resource_kwargs)

data = fh(**datum_kwargs)

#file_sizes = fh.get_file_sizes(datum_kwargs)
file_list = fh.get_file_list([datum_kwargs])