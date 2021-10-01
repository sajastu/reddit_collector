


import json
import os
import pickle
import random
import re
from bisect import bisect_left
from multiprocessing import cpu_count, Pool
from tqdm import tqdm
from somajo import SoMaJo
import spacy

# from rg_mesh import _rg_heat_2d

nlp = spacy.load('en_core_web_sm')
# nlp.add_pipe(nlp.create_pipe('sentencizer')) # updated
nlp.add_pipe('sentencizer')

random.seed(8080)

tokenizer = SoMaJo("en_PTB")

def _mp_m_process(param):
    ent = param
    src_sents = ent['document'].split('</s><s> ')
    src_tkns = []
    for sent in src_sents:
        src_sentences_tkns = tokenizer.tokenize_text([sent])
        ctr = 0
        for _ in src_sentences_tkns: ctr += 1
        src_sentences_tkns_tmp = []
        if ctr > 1:
            src_sentences_tkns = tokenizer.tokenize_text([sent])
            for sT in src_sentences_tkns:
                for tkn in sT:
                    src_sentences_tkns_tmp.append(tkn.text)
            src_sentences_tkns = src_sentences_tkns_tmp
        else:
            src_sentences_tkns = tokenizer.tokenize_text([sent])
            for sT in src_sentences_tkns:
                for tkn in sT:
                    src_sentences_tkns_tmp.append(tkn.text)
            src_sentences_tkns = src_sentences_tkns_tmp

        sent_tkns = []
        for token in src_sentences_tkns:
            sent_tkns.append(token)
        src_tkns.append(sent_tkns)

    summary_sents = ent['summary']
    tgt_sentences = [i for i in nlp(summary_sents).sents]

    tgt_sentences_tkns = tokenizer.tokenize_text([summary_sents])

    tgt_tkns = []
    for sentence in tgt_sentences_tkns:
        sent_tkns = []
        for token in sentence:
            sent_tkns.append(token.text)
        tgt_tkns.append(sent_tkns)
    srctkns = []
    for s in src_tkns:
        srctkns.extend(s)
    tgttkns = []
    for t in tgt_tkns:
        tgttkns.extend(t)
    return {
        'set': ent['set'],
        'src_tkns': srctkns,
        'tgt_tkns': tgttkns,
        'src_tkns_len': sum([len(s) for s in src_tkns]),
        'tgt_tkns_len': sum([len(t) for t in tgt_tkns]),
        'src_sent_len_list' : [len(s) for s in src_tkns],
        'tgt_sent_len_list' : [len(s) for s in tgt_tkns],
        'src_sents_len': len(src_sents),
        'tgt_sents_len': len(tgt_sentences),
        'id': ent['id']
    }

def bi_contains(lst, item):
    """ efficient `item in lst` for sorted lists """
    # if item is larger than the last its not in the list, but the bisect would
    # find `len(lst)` as the index to insert, so check that first. Else, if the
    # item is in the list then it has to be at index bisect_left(lst, item)
    return (item <= lst[-1]) and (lst[bisect_left(lst, item)] == item)


def mp_read(param):
    out = {}
    with open(param) as fR:
        for l in tqdm(fR):
            try:
                ent = json.loads(l.strip())
                out[ent['id']] = ent
            except:
                continue
    return out


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
idx = 0
id_files = id_files[:10]
for out in tqdm(pool.imap_unordered(mp_read, id_files), total=len(id_files)):
    if 'train' in id_files[idx]:
        set = 'train'
    elif 'val' in id_files[idx]:
        set = 'val'
    else:
        set='test'

    for k, v in out.items():
        v['set'] = set
        ds[k] = v
    idx+=1

pool.close()
pool.join()

pool = Pool(cpu_count())

ds_th_lst =[]
for id, ent in ds.items():
    ds_th_lst.append(ent)
    # _mp_m_process(ds_th_lst[-1])

##############################
ds_th={}
sents_len_list = []
# average_sents = [0, 0]

vocabulary = {
    'train': {},
    'val': {},
    'test': {}
}

src_stat = {
    'src_tkns_len': 0,
    'src_sents_len': 0,
    'src_sent_len_list': [],
}


tgt_stat = {
    'tgt_tkns_len': 0,
    'tgt_sents_len': 0,
    'tgt_sent_len_list': [],
}


counts= {
    'train': 0,
    'test': 0,
    'val': 0
}
tkns = {}
# import pdb;pdb.set_trace()
for out in tqdm(pool.imap_unordered(_mp_m_process, ds_th_lst), total=len(ds_th_lst)):
    """
    return {
        'set': ent['set'],
        'src_tkns': src_tkns,
        'tgt_tkns': tgt_tkns,
        'src_tkns_len': sum([len(s) for s in src_tkns]),
        'tgt_tkns_len': sum([len(t) for t in tgt_tkns]),
        'src_sent_len_list' : [len(s) for s in src_tkns],
        'tgt_sent_len_list' : [len(s) for s in tgt_tkns],
        'src_sents_len': len(src_sents),
        'tgt_sents_len': len(tgt_sentences),
        'id': ent['id']
    }

    """

    set = out['set']

    # adding to vocab
    whole_tokens = out['src_tkns'] + out['tgt_tkns']
    for tkn in whole_tokens:
        if tkn not in  vocabulary[set].keys():
            vocabulary[set][tkn] = 1
        else:
            vocabulary[set][tkn] += 1

    src_stat['src_tkns_len'] += out['src_tkns_len']
    src_stat['src_sents_len'] += out['src_sents_len']
    src_stat['src_sent_len_list'].extend(out['src_sent_len_list'])

    tgt_stat['tgt_tkns_len'] += out['tgt_tkns_len']
    tgt_stat['tgt_sents_len'] += out['tgt_sents_len']
    tgt_stat['tgt_sent_len_list'].extend(out['tgt_sent_len_list'])


    counts[set]+=1

    if sum([c for set, c in counts.items()]) % 2000 == 0:
        print(f'iteration {sum([c for set, c in counts.items()])//200000}')
        print(f'Count = {sum([c for set, c in counts.items()])}: train: {counts["train"]}, val: {counts["val"]}, test: {counts["test"]}')
        print(f'Average tokens ---> src {src_stat["src_tkns_len"] / sum([c for set, c in counts.items()])} '
              f'and tgt: {tgt_stat["tgt_tkns_len"] / sum([c for set, c in counts.items()])}')

        print(f'Average tokens ---> src {src_stat["src_sents_len"] / sum([c for set, c in counts.items()])} '
              f'and tgt: {tgt_stat["tgt_sents_len"] / sum([c for set, c in counts.items()])}')

        print(f'Compression ratio: {(src_stat["src_tkns_len"] [0] / sum([c for set, c in counts.items()])) / (tgt_stat["tgt_tkns_len"] [1] / sum([c for set, c in counts.items()]))}')


        print('------- Vocab Stats -------')

        for set in ["train", "val", "test"]:
            all_vocabs_for_set = vocabulary[set]
            print(f'{set}: count: {len(all_vocabs_for_set.keys())}')
            newDict = dict(filter(lambda elem: elem[1] >= 10, all_vocabs_for_set.items()))
            print(f'Occurring +10 times: count: {len(newDict.keys())}')




# if sum([c for set, count in counts.items()]) % len(ds_th_lst) == 0:
print(f'iteration {(sum([c for set, c in counts.items()]) // 200000 )+ 1}')
print(
    f'Count = {sum([c for set, c in counts.items()])}: train: {counts["train"]}, val: {counts["val"]}, test: {counts["test"]}')
print(f'Average tokens ---> src {src_stat["src_tkns_len"] / sum([c for set, c in counts.items()])} '
      f'and tgt: {tgt_stat["tgt_tkns_len"] / sum([c for set, c in counts.items()])}')

print(f'Average tokens ---> src {src_stat["src_sents_len"] / sum([c for set, c in counts.items()])} '
      f'and tgt: {tgt_stat["tgt_sents_len"] / sum([c for set, c in counts.items()])}')

print(
    f'Compression ratio: {(src_stat["src_tkns_len"][0] / sum([c for set, count in counts.items()])) / (tgt_stat["tgt_tkns_len"][1] / sum([c for set, count in counts.items()]))}')

print('Saving to pickle...')
if not os.path.exists("stats/"):
    os.makedirs("stats/")

pickle.dump({
    'src_stats': src_stat,
    'tgt_stats': tgt_stat,
    'vocabulary': vocabulary
}, open(f'stats/tldrHQ.pkl', mode='wb'))

pool.close()
pool.join()

# print(f'--------------')
# print(f'Count = {count}')
# print(f'Average tokens ---> src {tokens_count[0] / count} and tgt: {tokens_count[1] / count}')
# print(f'Average sents ---> src {average_sents[0] / count} and tgt: {average_sents[1]/ count}')
#


pool.close()
pool.join()
