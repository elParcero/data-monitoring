'''
The information for files are stored in our database as a resource and datum.


A resource contains the following:
{'id': '7d2d1b75-38fe-4e17-a88b-fb9e4a10b832',
 'path_semantics': 'posix',
 'resource_path': 'pizza_box_data/an_82ec23',
 'root': '/GPFS/xf08id/',
 'resource_kwargs': {'chunk_size': 1024},
 'spec': 'PIZZABOX_AN_FILE_TXT',
 'uid': '7d2d1b75-38fe-4e17-a88b-fb9e4a10b832'}

where:
'id':  is just some unique identifier
'path_semantics':  just signifies whether we're using "/" or "\" in our path
resource_path: this is the path of the file
root: this is the root directory of the file
resource_kwargs: these are arguments given to the resource
spec : this is just an identifier that identifies which file handler to use
    (for example this one we're creating for AN files will have the spec 
      "PIZZABOX_AN_FILE_TXT")
uid: another identifier

Whenever a file is opened, a file hander is called with these arguments:
fh = FileHander(resource_path=resource_path, **resource_kwargs)


Finally, a datum is the last piece of information to retrieve the file, it
looks like this:
{'_id': ObjectId('5ad624cae32635f0f1b4ef0c'),
 'datum_id': '2901c429-d497-49bf-b224-a76a2916de11',
 'datum_kwargs': {'chunk_num': 0},
 'resource': '7d2d1b75-38fe-4e17-a88b-fb9e4a10b832'}

where "_id" is some internal id, 'datum_id' is a unique identified for the
datum, 'datum_kwargs' are arguments for retreiving the datum and 'resource' is
the id of the resource this datum comes from.


This means that retrieving data takes a few steps:
1. First, we're given a datum id
2. We retrieve a datum given the datum id
3. from this datum id, we retrieve the resource
4. We use the resource to start a file handler:
    fh = FileHander(resource_path=resource_path, **resource_kwargs)
5. We then use the 'datum_kwargs' to get the data:
data = fh(**datum_kwargs)


For this exercise, we'll practice forwarding kwargs.

1. Create a resource and datum for one of the An files.
for the id, just use this:
import uuid
my_uid = str(uuid.uuid4())

this will create a unique id

2. Call the function from the previous exercise using the datum dictionary
ex:
fh = FileHandler(resource['resource_path'], **resource['resource_kwargs'])

hint:
if a = dict(foo="bar", foo2='bar2')
then
f(**a) is equivalent to f(foo='bar', foo2='bar2')

Give it a try and let me know if anything is unclear.

'''


