import glob
import json
import os
import re
import sys
from multiprocessing import Pool
from os.path import join as pjoin

import numpy as np
from rouge_score import rouge_scorer
from tqdm import tqdm
import spacy
from somajo import SoMaJo

tokenizer = SoMaJo("en_PTB")
nlp = spacy.load("en_core_web_lg")
nlp.disable_pipe("parser")
nlp.enable_pipe("senter")

BASE_DIR = 'mkdir -p /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized_extended/'

# if not os.path.exists(WRITE_DIR):
#     os.makedirs(WRITE_DIR)

def _get_ngrams(n, text):
    """Calcualtes n-grams.
    Args:
      n: which n-grams to calculate
      text: An array of tokens
    Returns:
      A set of n-grams
    """
    ngram_set = set()
    text_length = len(text)
    max_index_ngram_start = text_length - n
    for i in range(max_index_ngram_start + 1):
        ngram_set.add(tuple(text[i:i + n]))
    return ngram_set

def _get_word_ngrams(n, sentences):
    """Calculates word n-grams for multiple sentences.
    """
    assert len(sentences) > 0
    assert n > 0

    # words = _split_into_words(sentences)

    words = sum(sentences, [])
    # words = [w for w in words if w not in stopwords]
    return _get_ngrams(n, words)

def cal_rouge(evaluated_ngrams, reference_ngrams):
    reference_count = len(reference_ngrams)
    evaluated_count = len(evaluated_ngrams)

    overlapping_ngrams = evaluated_ngrams.intersection(reference_ngrams)
    overlapping_count = len(overlapping_ngrams)

    if evaluated_count == 0:
        precision = 0.0
    else:
        precision = overlapping_count / evaluated_count

    if reference_count == 0:
        recall = 0.0
    else:
        recall = overlapping_count / reference_count

    f1_score = 2.0 * ((precision * recall) / (precision + recall + 1e-8))
    return {"f": f1_score, "p": precision, "r": recall}

def greedy_selection(doc_sent_list, abstract_sent_list, summary_size):
    def _rouge_clean(s):
        return re.sub(r'[^a-zA-Z0-9 ]', '', s)

    max_rouge = 0.0
    abstract = sum(abstract_sent_list, [])
    abstract = _rouge_clean(' '.join(abstract)).split()
    sents = [_rouge_clean(' '.join(s)).split() for s in doc_sent_list]
    evaluated_1grams = [_get_word_ngrams(1, [sent]) for sent in sents]
    reference_1grams = _get_word_ngrams(1, [abstract])
    evaluated_2grams = [_get_word_ngrams(2, [sent]) for sent in sents]
    reference_2grams = _get_word_ngrams(2, [abstract])

    selected = []
    for s in range(summary_size):
        cur_max_rouge = max_rouge
        cur_id = -1
        for i in range(len(sents)):
            if (i in selected):
                continue
            c = selected + [i]
            candidates_1 = [evaluated_1grams[idx] for idx in c]
            candidates_1 = set.union(*map(set, candidates_1))
            candidates_2 = [evaluated_2grams[idx] for idx in c]
            candidates_2 = set.union(*map(set, candidates_2))
            rouge_1 = cal_rouge(candidates_1, reference_1grams)['f']
            rouge_2 = cal_rouge(candidates_2, reference_2grams)['f']
            rouge_score = rouge_1 + rouge_2
            if rouge_score > cur_max_rouge:
                cur_max_rouge = rouge_score
                cur_id = i
        if (cur_id == -1):
            # return selected
            break
        selected.append(cur_id)
        max_rouge = cur_max_rouge

    retrun_bin_labels = []
    for i, s in enumerate(doc_sent_list):
        if i in selected:
            retrun_bin_labels.append(1)
        else:
            retrun_bin_labels.append(0)

    return retrun_bin_labels, sorted(selected)

def sentencizer(text):
    sents = []
    doc = nlp(text)
    sent_lst = doc.sents
    for sent in sent_lst:
        sents.append(sent.text)
    return sents

def evaluate_rouge(hypotheses, references, type='f'):
    metrics = ['rouge1', 'rouge2', 'rougeL']
    scorer = rouge_scorer.RougeScorer(metrics, use_stemmer=True)
    results = {"rouge1_f": [], "rouge1_r": [], "rouge1_p": [], "rouge2_f": [],
               "rouge2_r": [], "rouge2_p": [], "rougeL_f": [], "rougeL_r": [], "rougeL_p": []}
    results_avg = {}

    if len(hypotheses) < len(references):
        print("Warning number of papers in submission file is smaller than ground truth file", file=sys.stderr)
    # import pdb;pdb.set_trace()
    hypotheses = list(hypotheses)
    references = list(references)
    for j, hyp in enumerate(hypotheses):
        submission_summary = hyp.replace('<q>', ' ')

        scores = scorer.score(references[j].strip(), submission_summary.strip())

        for metric in metrics:
            results[metric + "_f"].append(scores[metric].fmeasure)
            results[metric + "_r"].append(scores[metric].recall)
            results[metric + "_p"].append(scores[metric].precision)

        for rouge_metric, rouge_scores in results.items():
            results_avg[rouge_metric] = np.average(rouge_scores)

    return results_avg['rouge1_' + type], results_avg['rouge2_'+ type], results_avg['rougeL_'+ type]

def _mp_labelling(param):
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

    summary_sents = sentencizer(ent['summary'])
    tgt_sentences_tkns = tokenizer.tokenize_text(summary_sents)

    tgt_tkns = []
    for sentence in tgt_sentences_tkns:
        sent_tkns = []
        for token in sentence:
            sent_tkns.append(token.text)
        tgt_tkns.append(sent_tkns)

    ext_labels, selected_sent_ids = greedy_selection(src_tkns, tgt_tkns, 3)
    assert len(ext_labels) == len(src_sents), '`ext_labels` and `src_sents` should have same size'
    assert ext_labels is not None, '`ext_labels` should not be uninitialized'
    ent['ext_labels'] = ext_labels

    ext_summary = ' '.join([s for j, s in enumerate(src_sents) if j in selected_sent_ids])

    return ext_labels

def _mp_assigning_rg(param):
    with open(param, mode='r') as fR:
        ent = json.load(fR)
    src_sents = [s.strip() for s in ent['document'].split('</s><s> ')]
    rg_labels = []
    rg1_labels = []
    for sent in src_sents:
        result = evaluate_rouge([sent], [ent['summary']])
        # Extract a few results from ROUGE
        rg_labels.append((result[1] + result[2]) / 2)
        rg1_labels.append(result[0])
    if sum(rg_labels) > 0:
        rg_labels = [r / sum(rg_labels) for r in rg_labels]
    else:
        print(str(rg_labels) + ' ' + 'rg1 starts...')
        rg_labels = rg1_labels
        if sum(rg_labels) > 0:
            rg_labels = [r / sum(rg_labels) for r in rg_labels]
        else:
            print(str(rg_labels) + ' ' + 'rg1 not submitted...')
            rg_labels = [0 for r in rg_labels]
    ent['rg_labels'] = rg_labels
    ent['ext_labels'] = _mp_labelling(ent)
    return ent


old_instances = []
for f in glob.glob(BASE_DIR + "*.json"):
    old_instances.append(f)

pool = Pool(20)
counter = 0
datasets = []
for ent in tqdm(pool.imap_unordered(_mp_assigning_rg, old_instances), total=len(old_instances)):
    counter += 1
    datasets.append(ent)
    if counter % 25000 == 0:
        out_file = f'/mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized_extended_split/train.{int(counter/25000)}.json'
        with open(out_file, mode='w') as fW:
            for d in datasets:
                json.dump(d, fW)
                fW.write('\n')

if len(datasets) > 0:
    out_file = f'/mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized_extended_split/train.{int(counter / 25000)+1}.json'
    with open(out_file, mode='w') as fW:
        for d in datasets:
            json.dump(d, fW)
            fW.write('\n')