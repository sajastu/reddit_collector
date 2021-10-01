import pickle
import numpy as np

objects = {}
with (open("stats/tldrHQ.pkl", "rb")) as openfile:
    objects = pickle.load(openfile)


src_sent_len = np.asarray(objects['src_sent_len_list'])
tgt_sent_len = np.asarray(objects['tgt_sent_len_list'])
print('src')
print(np.mean(src_sent_len))
print(np.min(src_sent_len))
print(np.max(src_sent_len))

print('tgt')
print(np.mean(tgt_sent_len))
print(np.min(tgt_sent_len))
print(np.max(tgt_sent_len))