from databroker import Broker
db = Broker.named("chx")

from eiger_io.fs_handler import EigerHandler

db.reg.register_handler("AD_EIGER", EigerHandler)
db.reg.register_handler("AD_EIGER2", EigerHandler)

hdrs = iter(db(since="2016-08-19", until="2016-08-22", plan_name='count'))

# first two are bad, so I'm skipping them
hdr = next(hdrs)
hdr = next(hdrs)

# good one
hdr = next(hdrs)

# get the keys for images
# 'eiger4m_single_image' will be one of them for this data
keys = hdr.fields()

# one of them will be eiger4m_single_imags

# hdr.data() is an iterator and we turn it into a list
# it's a list of length 1 so we grab the next element with [0]
imgs = list(hdr.data('eiger4m_single_image'))[0]

# let's grab the first image
# this will get a numpy array for the image
img = imgs[0]

# print len of image:
print(f"Length of image is {len(imgs)}")

# try plotting
import matplotlib.pyplot as plt
plt.ion()

plt.imshow(img,vmin=0,vmax=100)

'''
Exercise 1:
    - try playing with plotting parameters:
        change vmin, vmax
        try different color maps:
             plt.set_cmap("Greys_r")
            type:
                plt.set_cmap("")
            to see all available color maps
    - plot the 1001th row of the image:
        plt.plot(img[:,1000])
    - plot the 1001th column of the image:
        plt.plot(img[1000, :])

Exercise 2:
    - the image here is a numpy array.
        Try to figure out how to take:
            - sum of whole image
            - some of one cross section

Exercise 3:
    - cycle through all the data and keep track of:
        - the number of images per header
        - the exposure time (hint, it's in hdr.start)

 

'''
