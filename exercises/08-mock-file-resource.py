'''
    Mock file resource

    In this example, we're going to save a file resource to databroker and try
    to retrieve it with a file handler

    You can run this from your laptop at home as well, so long as you install
    databroker.

    You'll create a resource as in the previous example, and try to retrieve it
    using the datum info
'''
from databroker.tests.utils import temp_config
from databroker import Broker
# this will create a temporary databroker object with nothing in it
db = Broker.from_config(temp_config)



from databroker.assets.handlers_base import HandlerBase
class MyHandler(HandlerBase):
    def __init__(self, *args, **kwargs):
        print("In init, received args : {}, kwargs {}".format(args, kwargs))

    def __call__(self, *args, **kwargs):
        print("In call, received args : {}, kwargs {}".format(args, kwargs))

# registering a handler according to a certain spec
db.reg.register_handler("ISSFILE", MyHandler)

# with the resource and datum you created in the previous example, try to input
# them as a resource and datum here:
# Hint : use "db.reg.register_resource?" for more help
# Also, register the resource first
db.reg.register_resource()
db.reg.register_datum()

# then retrieve them back by calling:
resource = db.reg.resource_given_datum_id(datum_id) # Supply the datum id here
datum_gen = db.reg.datum_gen_given_resource(datum_id) # Supply the datum id here
# just make it a list for now
datums = list(datum_gen)

# now instantiate your handler with the resource like this:
fh = MyHandler(resource_path=resource['resource_path'],
               **resource['resource_kwargs'])

# look at the first datum and use it to access the data
datum = datums[0]
data = fh(**datum_kwargs)

#you should be able to read your data
