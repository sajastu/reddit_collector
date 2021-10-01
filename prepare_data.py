import glob
import json
import os
import re
from multiprocessing import Pool

from somajo import SoMaJo
import spacy
from spacy.language import Language
from tqdm import tqdm

from transformers import AutoTokenizer

nlp = spacy.load("en_core_web_lg")
# nlp.disable_pipe("parser")
# nlp.enable_pipe("senter")

boundary = re.compile('^[0-9]$')

REMAP = {"-lrb-": "(", "-rrb-": ")", "-lcb-": "{", "-rcb-": "}",
         "-lsb-": "[", "-rsb-": "]", "``": '"', "''": '"'}

tokenizer = AutoTokenizer.from_pretrained(
    'facebook/bart-large',
    cache_dir=None,
    use_fast=False,
    revision='main',
    use_auth_token=None,
)


# @Language.component("my_component")
# def my_component(doc):
#     prev = doc[0].text
#     length = len(doc)
#     for index, token in enumerate(doc):
#         if (token.text == '.' and boundary.match(prev) and index != (length - 1)):
#             doc[index + 1].sent_start = False
#         prev = token.text
#     return doc


# nlp.add_pipe('my_component', before='parser')


def detokenize(tokens):
    out = []
    for token in tokens:
        if token.original_spelling is not None:
            out.append(token.original_spelling)
        else:
            out.append(token.text)
        if token.space_after:
            out.append(" ")
    return "".join(out)


def sentencizer(text):
    sents = []
    # tokenizer = SoMaJo("en_PTB", xml_sentences="s")
    # sentences = tokenizer.tokenize_text([text])
    # for sentence in sentences:
    #     for token in sentence:
    #         print(token.text)
    doc = nlp(text)
    sent_lst = doc.sents
    for sent in sent_lst:
        sents.append(sent.text)
    return sents

def clean(x):
    return re.sub(
        r"-lrb-|-rrb-|-lcb-|-rcb-|-lsb-|-rsb-|``|''",
        lambda m: REMAP.get(m.group()), x)


def _check_in_tokenizer(src):
    with tokenizer.as_target_tokenizer():
        labels = tokenizer([src], max_length=102400, padding=False, truncation=True,
                           target_side=True)

    label_splits = []
    label_split = []
    is_a_good_example = True
    modified = False
    for idx, id in enumerate(labels['input_ids'][0]):
        if id != 0 and id != 2:
            label_split.append(id)
        else:
            if idx > 0 and id == 2:
                label_splits.append(label_split.copy())
                label_split = []

    if len(label_splits) != len(src.split('</s><s> ')):
        print('inside bad example')
        # import pdb;pdb.set_trace()
        is_a_good_example=False
        return is_a_good_example, modified, None

    modified_sents = []
    for sent_text, s_ids in zip(src.split('</s><s> '), label_splits):
        if len(s_ids) > 2:
            modified_sents.append(sent_text.strip())
        else:
            modified = True
            continue

    modified_src = '</s><s> '.join(modified_sents)

    return is_a_good_example, modified, modified_src

def load_json(p, lower):
    source = []
    tgt = []
    flag = False
    for sent in json.load(open(p))['sentences']:
        tokens = [t['word'] for t in sent['tokens']]
        if (lower):
            tokens = [t.lower() for t in tokens]
        if (tokens[0] == '@highlights'):
            flag = True
            tgt.append([])
            continue
        if (flag):
            tgt[-1].extend(tokens)
        else:
            source.append(tokens)

    source = [clean(' '.join(sent)).split() for sent in source]
    tgt = [clean(' '.join(sent)).split() for sent in tgt]
    return source, tgt, p.split('/')[-1]

def _mp_sentencizer(param):
    tokenized_fname = param

    document, tgt, id = load_json(tokenized_fname, lower=True)
    document = [' '.join(d) for d in document]
    src_sents = []
    for i, sent in enumerate(document):
        if len(sent.strip()) > 0 and len(sent.strip().split()) > 2:
            src_sents.append(sent.strip())

    src = '</s><s> '.join(src_sents)

    src = src.strip()
    reddit_post = {}
    # check to see if Contextualized tokenizer also can handle the sentences...

    is_a_good_example, modified, new_src = _check_in_tokenizer(src)

    if is_a_good_example and len(src.strip()) > 0:
        while modified:
            print('modified loop')
            is_a_good_example, modified, new_src = _check_in_tokenizer(new_src)
            if not is_a_good_example:
                print(f'bad example after modification {reddit_post["id"]}')
                return reddit_post['id']

        reddit_post['document'] = new_src
        reddit_post['summary'] = ' '.join([' '.join(t) for t in tgt])
        reddit_post['id'] = id
        return reddit_post
    else:
        # print(f'bad example {reddit_post["id"]}')
        return None


tokenized_files = []
for f in glob.glob('/mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized/*.json'):
    tokenized_files.append(f)


pool = Pool(20)
counter = 0
chunk_size = 1
datasets = []
for ent in tqdm(pool.imap_unordered(_mp_sentencizer, tokenized_files), total=len(tokenized_files)):
    if ent is not None:
        # datasets.append(ent)
        counter+=1
        # if counter % 25000 == 0:
        # out_file = f'/disk1/sajad/datasets/reddit/tldrQ/tldr2021/dataset-m6/train.{int(counter/25000)}.json'
        out_file = f'/mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized_extended/{ent["id"]}'
        with open(out_file, mode='w') as fW:
            # for d in datasets:
            json.dump(ent, fW)
            fW.write('\n')

# if len(datasets) > 0:
#     out_file = f'/disk1/sajad/datasets/reddit/tldrQ/tldr2021/dataset-m6/train.{int(counter / 25000)+1}.json'
#     with open(out_file, mode='w') as fW:
#         for d in datasets:
#             json.dump(d, fW)
#             fW.write('\n')