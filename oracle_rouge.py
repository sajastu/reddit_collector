"""
    ``Copied from https://github.com/nlpyang/PreSumm/blob/master/src/prepro/data_builder.py''

    Computing oracle ROUGE for extractive summarization
    Note that the script assumes source sentences are seperated using `<s></s>` delimiter. For more info, refer to
    multiSumm/prepare_data.py

"""
import argparse
import json
import os
import re
from multiprocessing import Pool
from os.path import join as pjoin

from datasets import load_metric
from somajo import SoMaJo
# import spacy
from tqdm import tqdm

# nlp = spacy.load("en_core_web_lg")
# nlp.disable_pipe("parser")
# nlp.enable_pipe("senter")
# tokenizer = SoMaJo("en_PTB")
# metric = load_metric("rouge")


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

    # return sorted(selected)
    return retrun_bin_labels, sorted(selected)


def sentencizer(text):
    sents = []
    doc = nlp(text)
    sent_lst = doc.sents
    for sent in sent_lst:
        sents.append(sent.text)
    return sents

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

    ext_labels, selected_sent_ids = greedy_selection(src_tkns, tgt_tkns, args.n_sents)
    assert len(ext_labels) == len(src_sents), '`ext_labels` and `src_sents` should have same size'
    assert ext_labels is not None, '`ext_labels` should not be uninitialized'
    ent['ext_labels'] = ext_labels

    ext_summary = ' '.join([s for j, s in enumerate(src_sents) if j in selected_sent_ids])

    return ent, ext_summary

def _run(args):
    if len(args.split) > 0:
        pass
    else:
        for set in ['test', 'validation', 'train']:
            old_instances = []
            new_instances = []
            old_ids = []
            new_ids = []
            with open(pjoin(args.base_dir, set + '.json')) as fR:
                num_lines = sum(1 for _ in open(pjoin(args.base_dir, set + '.json'), 'r'))
                for l in tqdm(fR, total=num_lines, desc=f"labelling {pjoin(args.base_dir, set + '.json')}"):
                    ent = json.loads(l.strip())
                    old_instances.append(ent)
                    old_ids.append(ent['id'])
                pool = Pool(80)
                ext_summaries = []
                true_summaries = []
                for ent in tqdm(pool.imap_unordered(_mp_labelling, old_instances), total=len(old_instances)):
                    ent_m, extsummary = ent
                    true_summary = ent_m['summary']
                    true_summaries.append(true_summary)
                    ext_summaries.append(extsummary)
                    new_instances.append(ent_m)
                    new_ids.append(ent_m['id'])

                if set != 'train':
                    print(f'Predicting oracle scores... for {set}')
                    result = metric.compute(predictions=ext_summaries, references=true_summaries, use_stemmer=True)
                    # Extract a few results from ROUGE
                    result = {key: value.mid.fmeasure for key, value in result.items()}
                    print(f'RG-1: {result["rouge1"]} -- RG-2: {result["rouge2"]} -- RG-L: {result["rougeL"]}')

                n_new_instances = []
                for oId in old_ids:
                    new_index = new_ids.index(oId)
                    n_new_instances.append(new_instances[new_index])

            with open(pjoin(args.write_dir, set + '.json'), mode='w') as fW:
                for n_inst in n_new_instances:
                    json.dump(n_inst, fW)
                    fW.write('\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-n_sents", default=6, type=int, help='Max number of sentences to be labeled')
    parser.add_argument("-base_dir", default='/home/code-base/user_space/packages/datasets/reddit_tifu_segmented-enhanced/',
                        type=str, help='Dataset\'s base dir')
    parser.add_argument("-write_dir", default='/home/code-base/user_space/packages/datasets/reddit_tifu_enhanced-ext/',
                        type=str, help='Dataset\'s base dir')
    parser.add_argument("-set", default='', type=str, help='Set name; if not specified, will get through all sets')
    args = parser.parse_args()

    if not os.path.exists(args.write_dir):
        os.makedirs(args.write_dir)

    _run(args)
