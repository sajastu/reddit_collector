import glob
import os
from os.path import join as pjoin

def _get_zero_files(store_path):
    out_zero = []
    for s in glob.glob(pjoin(store_path, "*")):
        file_size = os.path.getsize(s)
        if file_size < 10:
            out_zero.append(s.split('/')[-1])

    return out_zero