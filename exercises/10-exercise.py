'''
Author: Jorge Diaz Jr

Exercise 10
-----------
Get file size

'''
import humanize

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
        return ['{name}_{index}.npy'.format(name=self._name, index=kwargs['index']) #**kwargs
                for kwargs in datum_kwarg_gen]


events = []
datum_ids = []

db = Broker.from_config(temp_config())

reg_handler = db.reg.register_handler("NPY_SEQ", NumpySeqHandler)

# img is a simulated detector. It needs to know where reg is located
RE = RunEngine({})

# subscribe writing to database to the RunEngine
RE.subscribe(db.insert)

# counting images
for i in range(10):
    uid, = RE(count([img, det1, det2]))

#extracting the events from each header doc in the files generated
for i in range(10):
    hdr = db[-i - 1]
    event = hdr.events()
    events.append(next(event))

#extracting the datum ids from each event that was generated
for i in range(len(events)):
    datum_ids.append(events[i]['data']['img'])

resources = list()
# call these functions to get the resource 
# and store them in a resources list 
for i in range(len(datum_ids)):
    resource = db.reg.resource_given_datum_id(datum_ids[i])
    resources.append(resource)

sizes = []
# initialize a file handler
# then retrieve the size for that specific file
for resource in resources:
    fh = NumpySeqHandler(resource['resource_path'], resource['root'])
    datum_gen = db.reg.datum_gen_given_resource(resource)
    datum_kwargs_list = [datum['datum_kwargs'] for datum in datum_gen]
    file_list = fh.get_file_list(datum_kwargs_list)
    file_sizes = sum([os.path.getsize(filename) for filename in file_list])
    sizes.append(file_sizes)

print(sizes)
