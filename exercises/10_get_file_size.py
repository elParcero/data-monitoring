'''
Exercise 10:
1. Run the code as is.
2. Look at the documents generated:
docs = db[-1].documents()
next(docs)
next(docs)
next(docs)
# etc
You can see a start, descriptor, event , stop etc
3. Get the datum from the last generated documents:
hdr = db[-1]
See if you can figure it out
4. Get the resource from this datum
5. Instantiate the file handler from this datum
6. Define a new method called "get_file_size", that upon receiving a generator
of datums, returns a file size
Hint: you can start off by creating a custom function that only receives
information from one datum. Once this works, it can be turned into a generator
for multiple datums.
'''
import numpy as np
import os
from functools import wraps

# Some bluesky modules
from bluesky import RunEngine
from bluesky.plans import count

# simulated devices from ophyd
from ophyd.sim import (img, det1, det2, NumpySeqHandler, motor1,
                       SynSignalWithRegistry)

# databroker modules
from databroker.tests.utils import temp_config
from databroker import Broker


class NumpySeqHandler:
    specs = {'NPY_SEQ'}

    def __init__(self, filename, root=''):
        self._name = os.path.join(root, filename)


    def __call__(self, index):
        return np.load('{}_{}.npy'.format(self._name, index))


    def get_file_list(self, datum_kwarg_gen):
        #This method is optional. It is not needed for access, but for export.
        return ['{name}_{index}.npy'.format(name=self._name, **kwargs)
                for kwargs in datum_kwarg_gen]


    def get_file_size(self, datum_kwarg_gen):
        resource = db.reg.resource_given_datum_id(datum_kwarg_gen['datum_id'])
        fpath = resource['root'] + "/" + resource['resource_path'] + \
            '_' + str(datum_kwarg_gen['datum_kwargs']['index']) + '.npy'
        size = os.path.getsize(fpath)
        return size
    '''
    # define a function:
    def get_file_size(self, datum_kwarg_gen):
        # get the file size for this datum
        #for datum in datum_kwarg_gen:
            # use this info to get file size
    return 1
    '''

db = Broker.from_config(temp_config())
reg_handler = db.reg.register_handler("NPY_SEQ", NumpySeqHandler)
# img is a simulated detector. It needs to know where reg is located


RE = RunEngine({})

# subscribe writing to database to the RunEngine
RE.subscribe(db.insert)


# count an image
uid, = RE(count([img, det1, det2]))

# now run db[-1] etc
hdr = db[-1]
events = db.get_events(hdr)
event = next(events)


datum_id = event['data']['img']
# call these functionos to get the resource and datum
resource = db.reg.resource_given_datum_id(datum_id)
datum_gen = db.reg.datum_gen_given_resource(resource)
datum = next(datum_gen)

# initialize a file handler
fh = NumpySeqHandler(resource['resource_path'], resource['root'])
size = fh.get_file_size(datum)
print('File size is ' +str(size)+ 'B')

