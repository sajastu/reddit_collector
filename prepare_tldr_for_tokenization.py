import glob

# all_ids = []
# for f in glob.glob('//home/code-base/mashine_split_lrg_reddit/m_1/*'):
#     all_ids.append(f.split('/')[-1].replace('.json', ''))
#
# with open('m1_ids.txt', mode='w') as fW:
#     for a in all_ids:
#         fW.write(a)
#         fW.write('\n')
#
import json
from bisect import bisect_left
from multiprocessing import Pool, cpu_count

from tqdm import tqdm

def bi_contains(lst, item):
    """ efficient `item in lst` for sorted lists """
    # if item is larger than the last its not in the list, but the bisect would
    # find `len(lst)` as the index to insert, so check that first. Else, if the
    # item is in the list then it has to be at index bisect_left(lst, item)
    return (item <= lst[-1]) and (lst[bisect_left(lst, item)] == item)

def _mp_write(params):
    fname = params

    with open(fname ,mode='r') as fR:
        ent = json.load(fR)

    fname = fname.split('/')[-1]
    src = ent['src'].replace('\n',' ')
    tgt = ent['tldr'].replace('\n',' ')

    to_be_witten = src.encode("ascii", "ignore").decode() + '\n' + '@highlights' + '\n' + tgt.encode("ascii", "ignore").decode()
    if not ' ich ' in to_be_witten or not 'ich ' in to_be_witten:
        with open('/mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_highlights/' + fname, mode='w') as fW:
                fW.write(to_be_witten.strip())
    # else:
    #     con

files = []
for f in glob.glob("/mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021/*"):
    files.append(f)
    # _mp_write(f)

pool = Pool(cpu_count())
for _ in tqdm(pool.imap_unordered(_mp_write, files), total=len(files)):
    pass
