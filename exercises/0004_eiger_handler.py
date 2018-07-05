import os
import numpy as np
#from databroker.assets.handlers import AreaDetectorTiffHandler


class AreaDetectorTiffHandler():
    specs = {'AD_TIFF'}

    def __init__(self, fpath, template, filename, frame_per_point=1):
        self._path = os.path.join(fpath, '')
        self._fpp = frame_per_point
        self._template = template
        self._filename = filename

    def _fnames_for_point(self, point_number):
        start = int(point_number * self._fpp)
        stop = int((point_number + 1) * self._fpp)
        for j in range(start, stop):
            yield self._template % (self._path, self._filename, j)

    def __call__(self, point_number):
        import tifffile
        print(point_number)
        ret = []
        for fn in self._fnames_for_point(point_number):
            with tifffile.TiffFile(fn) as tif:
                ret.append(tif.asarray())
        return np.array(ret).squeeze()

    def get_file_list(self, datum_kwargs):
        ret = []
        for d_kw in datum_kwargs:
            ret.extend(self._fnames_for_point(**d_kw))
        return ret

def get_file_size(file_list):
    sizes = []
    for file in file_list:
        sizes.append(os.path.getsize(file))
    return sizes

resource_path = '/data/chx/tiff'
resource_kwargs = {'template': '%s%s_%6.6d.tiff', 
                   'filename': '6ef23c84-d8d5-4157-a0ee', 
                   'frame_per_point': 1}
datum_kwargs = {'point_number' : 0}

fh = AreaDetectorTiffHandler(resource_path, **resource_kwargs)

data = fh(**datum_kwargs)

file_list = fh.get_file_list([datum_kwargs])
file_size = get_file_size(file_list)