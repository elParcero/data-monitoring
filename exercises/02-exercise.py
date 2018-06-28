'''
Author: Jorge Diaz
'''
from databroker import Broker
import pandas as pd
import numpy as np

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
imgs = next(hdr.data('eiger4m_single_image'))

# let's grab the first image
# this will get a numpy array for the image
img = imgs[0]
num_imgs = []
exposure_time = []

# print len of image:
print(f"Length of image is {len(imgs)}")

# try plotting
import matplotlib.pyplot as plt
plt.ion()
plt.figure(7)
plt.clf()
plt.imshow(img,vmin=0,vmax=100)
#plt.figure(8)
#plt.clf()
#plt.plot(img[:, 1000])

for hdr in hdrs:
    start_document = hdr.start
    try:
        exposure_time.append(start_document['exposure_time'])
        num_imgs.append(len(imgs))
    except (IndexError, KeyError):
        continue

print(num_imgs)
print(exposure_time)

chx_info = {"Number of images" : num_imgs, "Exposure Time" : exposure_time}
df = pd.DataFrame(chx_info)
print(df)

num_imgs_a = np.array(num_imgs)
expo_time = np.array(exposure_time)

plot_expo_time, time_bins  = np.histogram(expo_time, bins = 100, range = (0, 600))

bin_centers = []

bin_centers = (time_bins[ : -1 ] + time_bins[ 1 : ]) * .5

plt.figure(0)
plt.clf()
plt.plot(bin_centers, plot_expo_time)

plt.figure(1)
plt.clf()
plt.hist(expo_time, bins = 100, range = (0, 600))


#print sum of image
print("Sum of image is " + str(np.sum(img)))
print("Sum of image is for index 77 = " + str(np.sum(img[77])))

