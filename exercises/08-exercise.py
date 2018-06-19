'''
Author: Jorge Diaz
Exercise 8
'''
import uuid
from databroker.tests.utils import temp_config
from databroker import Broker
from databroker.assets.handlers_base import HandlerBase

# this will create a temporary databroker object with nothing in it
db = Broker.from_config(temp_config())

class MyHandler(HandlerBase):
    def __init__(self, *args, **kwargs):
        print("In init, received args : {}, kwargs {}".format(args, kwargs))


    def __call__(self, *args, **kwargs):
        print("In call, received args : {}, kwargs {}".format(args, kwargs))


def get_resource(resource_uid, filepath):
	resource = {'id': resource_uid,
 	'path_semantics': 'posix',
 	'resource_path': filepath,
 	'root': '/home/jdiaz',
 	'resource_kwargs': {},
 	'spec': 'PIZZABOX_AN_FILE_TXT',
 	'uid': resource_uid,
	}
	return resource


def get_datum(datum_uid, resource_uid):
	datum = {'datum_id': datum_uid,
 	'datum_kwargs': {},
 	'resource': resource_uid
 	}
	return datum


filepath = "/home/jdiaz/projects/data-monitoring/data/iss_sample_data/an_2f218f"

resource_uid = str(uuid.uuid4()) #create a unique id for resource
datum_uid = str(uuid.uuid4()) #create a unique id for datum
resource = get_resource(resource_uid, filepath) # retrieving dict for resource
datum = get_datum(datum_uid, resource['uid']) # retrieving dict for datum

# registering a handler according to a certain spec

<<<<<<< Updated upstream
db.reg.insert_resource( "PIZZABOX_AN_FILE_TXT", resource_path = resource['resource_path'], resource_kwargs= resource['resource_kwargs'])
db.reg.insert_datum(resource = datum['resource'], datum_id = datum['datum_id'], datum_kwargs = datum['datum_kwargs'])

db.reg.register_resource('PIZZABOX_AN_FILE_TXT', root = resource['root'], rpath = resource['resource_path'], rkwargs = resource['resource_kwargs'])
db.reg.register_datum(resource_uid = datum['resource'], datum_kwargs = datum['datum_kwargs'])

db.reg.register_handler("PIZZABOX_AN_FILE_TXT", MyHandler)

#resource = db.reg.resource_given_datum_id(datum['datum_id']) # Supply the datum id here
=======
new_resource = db.reg.insert_resource( "PIZZABOX_AN_FILE_TXT", resource_path = resource['resource_path'], \
	resource_kwargs = resource['resource_kwargs'], uid = resource['uid'])

new_datum = db.reg.insert_datum(resource = datum['resource'], datum_id = datum['datum_id'],\
 datum_kwargs = datum['datum_kwargs'])

db.reg.register_handler("PIZZABOX_AN_FILE_TXT", MyHandler)

resource = db.reg.resource_given_datum_id(new_datum['datum_id']) # Supply the datum id here
print(resource)
>>>>>>> Stashed changes
