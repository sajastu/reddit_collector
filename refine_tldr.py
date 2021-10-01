import glob
import json
import os
from multiprocessing import cpu_count, Pool

from tqdm import tqdm


def _mp_filter(param):
    with open(param) as fR:
        for l in fR:
            ent = json.loads(l.strip())
            tldr_splited = ent['tldr'].split(' ')
            if ':' in tldr_splited[0].strip() or '*' in tldr_splited[0].strip() \
                or ';' in tldr_splited[0].strip():
                ent['tldr'] = ' '.join([s.strip() for s in tldr_splited[1:]])
                with open(param, mode='w') as fW:
                    json.dump(ent, fW)

files = []
for f in glob.glob("/mnt/ilcompn0d1/user/sotudehg0/tldr_data/*.json"):
    files.append(f)
#     _mp_filter(f)


all_tldrs = []
pool = Pool(cpu_count())
for _ in tqdm(pool.imap_unordered(_mp_filter, files), total=len(files)):
    pass

