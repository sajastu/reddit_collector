import json
import os.path
from tqdm import tqdm

from multiprocessing import cpu_count, Pool

def mp_change(params):
    out = {}
    split_id = ''
    with open(params) as fR:
        for l in tqdm(fR):
            ent = json.loads(l.strip())
            if 'train' in params:
                split_id = 'train'
            elif 'valid' in params:
                split_id = 'valid'
            elif 'test'in params:
                split_id = 'test'
            ent['id'] = split_id + "-TLDR" + ent['id'].split("TLDR")[1].replace('.json', '')

    return out, params

def mp_write(params):
    wr_dir, ent = params

    with open(wr_dir, mode='a') as fw:
        json.dump(fw)
        fw.write('\n')

# for j, d in enumerate(['m0', 'm1', 'm2', 'm3', 'm4', 'm2021', 'm6']):
ds = {}
id_files = []
# for j, d in enumerate(['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6']):
c=0
    # for root, dirs, files in os.walk(f'/disk1/sajad/datasets/reddit/tldrQ/chunks/dataset-{d}', topdown=False):
# for root, dirs, files in os.walk(f'/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldr-9+/full-reddit/', topdown=False):
for root, dirs, files in os.walk(f'/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrQ/', topdown=False):
    for name in files:
        if '.json' in name:
            id_files.append(os.path.join(root, name))

print('reading entire dataset...')
pool = Pool(cpu_count())
files_all = []
for out in tqdm(pool.imap_unordered(mp_change, id_files), total=len(id_files)):
    files_all.append((out[1], out[0]))


pool.close()
pool.join()

# Now saving the revised ds


if not os.path.exists("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/"):
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m0/")
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m1/")
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m2/")
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m3/")
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m4/")
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m5/")
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m6/")
    os.makedirs("/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldrHQ/dataset-m2021/")


pool_n = Pool(cpu_count())

for _ in tqdm(pool_n.imap_unordered(mp_write, files_all), total=len(files_all)):
    pass


pool_n.close()
pool_n.join()
