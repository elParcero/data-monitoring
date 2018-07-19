import os
# conda install scikit-beam
from skbeam.core.accumulators.histogram import Histogram

min_size = 0
max_size = 1e10 # 10 GB here
Nbins = 1e4
h = Histogram((Nbins, min_size, max_size))

# at each iteration for a file size fill histogram
h.fill(file_size)

#to plot histogram values
plt.plot(h.centers, h.values)
# the bin centers 