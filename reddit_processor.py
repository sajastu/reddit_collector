import argparse
import glob
import json
import multiprocessing
import os
import pickle
import re
from multiprocessing import Pool
from os.path import join as pjoin

from tqdm import tqdm

from Reddit import Reddit

pattern = re.compile("[\s\.]+tl\s*.{0,3}\s*dr\s?")


class RedditProcessor(Reddit):
    def __init__(self, args):
        super(RedditProcessor, self).__init__(args)
        self.args = args

        # self.post_comments = pickle.load(open(self.args.comments_file, 'rb'))
        # self.post_comments = {k.split('_')[1]: v for k, v in self.post_comments.items()}
        self.tldr_keywords = eval(
            '["tl;dr;", "tl dr", "tl;dr", "tldr", "tl:dr", "tl/dr", "tl; dr", "tl,dr", '
            '"tl, dr", "tl-dr", "tl’dr", "tl: dr", "tl.dr", "tl ; dr", "tl dr", '
            '"tldr;dr", "tl ;dr", "tl\dr", "tl/ dr", "tld:dr", "tl;;dr", "tltl;dr", '
            '"tl˜dr", "tl / dr", "tl :dr", "tl - dr", "tl\\dr", "tl. dr", "tl:;dr", '
            '"tl|dr", "tl;sdr", "tll;dr", "tl : dr", "tld;dr"]')

    def _process_bulk(self, param):

        def _have_tldr(text):
            for i, kw in enumerate(self.tldr_keywords):
                if kw in text.lower():
                    return i
            # tldr splitter not found
            text = text.lower()
            result = pattern.findall(text.lower())
            if len(result) == 1:
                # new splitter should be added to the self.tldr_keywords
                # self.tldr_keywords.append(result[0])
                with open('tldr_kw.txt', mode='a') as fR:
                    fR.write(result[0])
                    fR.write('\n')

                return result[0]

            return -1

        def refine_tldr(tldr):
            tldr_splited = tldr.split(' ')
            if ':' in tldr_splited[0].strip() or '*' in tldr_splited[0].strip() \
                    or ';' in tldr_splited[0].strip() or ',' in tldr_splited[0].strip()\
                    or '-' in tldr_splited[0].strip():
                tldr = ' '.join([s.strip() for s in tldr_splited[1:]])
            else:
                tldr = ' '.join([s.strip() for s in tldr_splited])

            return tldr

        def get_tldr_from_post(post_body, kw_idx):

            # post_text = reddit_post['selftext'].lower() # for submissions
            post_text = post_body.lower() # for comments

            p_text = post_text.split(self.tldr_keywords[kw_idx])[0]
            tldr = post_body.lower().split(self.tldr_keywords[kw_idx])[1]

            tldr = refine_tldr(tldr)

            return p_text, tldr

        def get_tldr_from_post_str_splitter(post_body, splitter):
            # post_text = reddit_post['selftext'].lower() # for submissions
            post_text = post_body.lower()  # for comments

            p_text = post_text.split(splitter)[0]
            tldr = post_body.lower().split(splitter)[1]

            tldr = refine_tldr(tldr)

            return p_text, tldr

        tldr_portion_posts = []
        processed = 0
        try:
            with open(param, mode='r') as rsFile:
                # num_lines = sum([1 for _ in open(param)])
                # print(num_lines)
                # for line in tqdm(rsFile, total=num_lines):
                for line in tqdm(rsFile):
                    processed += 1
                    reddit_post = json.loads(line)
                    # post_body = reddit_post['body'].replace('\n', ' ').lower()
                    post_body = reddit_post['selftext'].replace('\n', ' ').lower()
                    kw_idx = _have_tldr(post_body) # for comments
                    if isinstance(kw_idx, int) and kw_idx > -1:
                        post_text, tldr = get_tldr_from_post(post_body, kw_idx)
                        if len(tldr.split(' ')) > 4 and len(tldr.split(' ')) < len(post_text.split(' ')):
                            tldr_portion_posts.append(
                                {
                                'title':reddit_post['title'].strip() if 'title' in reddit_post.keys() else None,
                                # 'title': None,
                               'src': post_text.strip(),
                               'tldr': tldr.strip(),
                               'date': param.split('/')[-1]
                                }
                            )

                            self._mp_write((
                                param.split('/')[-1] + f'-cm-{len(tldr_portion_posts)}',
                                {
                                    'title': reddit_post['title'].strip() if 'title' in reddit_post.keys() else None,
                                    # 'title': None,
                                    'src': post_text.strip(),
                                     'tldr': tldr.strip(),
                                     'date': param.split('/')[-1]
                                }
                            ))

                    elif kw_idx != -1 and isinstance(kw_idx, str):
                        # splitter is returned
                        post_text, tldr = get_tldr_from_post_str_splitter(post_body, kw_idx)
                        if len(tldr.split(' ')) > 4 and len(tldr.split(' ')) < len(post_text.split(' ')):
                            tldr_portion_posts.append(
                                {
                                    'title':reddit_post['title'].strip() if 'title' in reddit_post.keys() else None,
                                    # 'title': None,
                                    'src': post_text.strip(),
                                    'tldr': tldr.strip(),
                                    'date': param.split('/')[-1]
                                }
                            )

                            self._mp_write((
                                param.split('/')[-1] + f'-cm-{len(tldr_portion_posts)}',
                                {
                                    'title': reddit_post['title'].strip() if 'title' in reddit_post.keys() else None,
                                    # 'title': None,
                                    'src': post_text.strip(),
                                    'tldr': tldr.strip(),
                                    'date': param.split('/')[-1]
                                }
                            ))

                    else:
                        continue
            return tldr_portion_posts, processed

        except Exception as e:
            with open('error_preprocess.txt', mode='a') as fA:
                fA.write(param + '\t' + str(e))
                fA.write('\n')

            return [], 0

    def _mp_write(self, param):
        j = param[0]
        tldr_dict = param[1]
        with open(self.args.write_dir + f'/TLDR_{j}.json', mode='w') as fW:
            json.dump(tldr_dict, fW)

    def _run(self):
        """
            # reading bulk files from `args.base_dir` into a list
            # this list will then be used for multiprocessing
            # each CPU will process one bulk data, and after processing,
            # it will write a single file to the `args.write_dir` directory.
        """


        # bulk_files = []

        # with open('error_preprocess_init.txt') as fR:
        #     num_lines = sum([1 for _ in open('error_preprocess_init.txt')])
        #     for l in tqdm(fR, total=num_lines):
        #         src_file_dir = l.split('\t')[0].strip()
        #         if src_file_dir not in bulk_files:
        #             bulk_files.append(src_file_dir)
                    # p = multiprocessing.Pool(4)
                    # src_file_name = src_file_dir.split('/')[-1]
                    # try:
                    #     p.map(os.remove, glob.glob(f'/home/sotudehg0/tldr_data_comments_full/TLDR_{src_file_name}-cm-*.json'))
                    # except:
                    #     print("No file to delete! :)")
        ctr_processed = 0
        ctr_processed_written = 0

        # for f in ["/mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/RC_2020-04-26"]:
        #     processed_reddit = self._process_bulk(f)
        #     ctr_processed += processed_reddit[1]
        #     if len(processed_reddit[0]) > 0:
        #         ctr_processed_written += len(processed_reddit[0])

        # pool = Pool(self.args.n_cpus)
        # for processed_reddit in tqdm(pool.imap_unordered(self._process_bulk, bulk_files), total=len(bulk_files)):
        #     ctr_processed += processed_reddit[1]
        #
        #     if len(processed_reddit[0]) > 0:
        #         ctr_processed_written += len(processed_reddit[0])
        #
        # pool.close()
        # pool.join()
        #
        # print('------------')
        # print(f'{ctr_processed} reddit posts processed, of which {ctr_processed_written}  '
        #       f'(i.e., {round((ctr_processed_written / ctr_processed) * 100, 2)}%)  is written into {self.args.write_dir}')
        #
        # print('-------------')


        ##################################################

        bulk_files = []
        visited_names = []
        for root, dirs, files in os.walk(self.args.read_dir, topdown=False):
            for name in files:
                if '.' not in name and name not in visited_names and 'RS' in name:
                    bulk_files.append(os.path.join(root, name))
                    visited_names.append(name)

        print(f'Processing {len(bulk_files)} bulk files started...')
        ctr_processed = 0
        ctr_processed_written = 0

        # try:
        # for f in tqdm(bulk_files, total=len(bulk_files)):
        #     reddit_post = self._process_bulk(f)

        # for f in ["/mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/RC_2020-04-26"]:
        #     processed_reddit = self._process_bulk(f)
        #     ctr_processed += processed_reddit[1]
        #     if len(processed_reddit[0]) > 0:
        #         ctr_processed_written += len(processed_reddit[0])


        #
        all_tldrs = []
        pool = Pool(self.args.n_cpus)
        for processed_reddit in tqdm(pool.imap_unordered(self._process_bulk, bulk_files), total=len(bulk_files)):
            ctr_processed += processed_reddit[1]

            if len(processed_reddit[0]) > 0:
                ctr_processed_written += len(processed_reddit[0])
                all_tldrs.extend(processed_reddit[0])


        pool.close()
        pool.join()


        print('------------')
        print(f'{ctr_processed} reddit posts processed, of which {ctr_processed_written}  '
              f'(i.e., {round((ctr_processed_written / ctr_processed) * 100, 2)}%)  is written into {self.args.write_dir}')

        print('-------------')

        if os.path.isfile('all_data_count.json'):
            with open('all_data_count.json') as fR:
                data_count = json.load(fR)

            data_count['submissions']['2021'] += ctr_processed
            with open('all_data_count.json', mode='w') as fW:
                json.dump(data_count, fW)

        # pool_wr.join() /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/RC_2020-04-26



    def _process_bulk_enumerate(self, file_dir):
        def get_year(id):
            end_year = 2023
            curr_year = 2004
            while curr_year < int(end_year):
                if '_' + str(curr_year) in id:
                    return str(curr_year)
                curr_year += 1
            return '2021'

        with open(file_dir) as fR:
            total_lines = sum([1 for _ in fR])

        id = file_dir.split('/')[-1]
        is_RS = ('rs' in id.lower())
        return is_RS, total_lines, get_year(id), id


    def _run_enumerate(self):
        bulk_files = []
        visited_names = []
        c=0

        if os.path.isfile('all_data_count.json'):
            with open('all_data_count.json') as fR:
                all_count = json.load(fR)

        else:
            all_count = {
                'submissions':
                    {},
                'comments':
                    {},
                'explored':
                    []
            }
        for root, dirs, files in os.walk(self.args.read_dir, topdown=False):
            for name in files:
                if '.' not in name and name not in visited_names and name not in all_count['explored']:
                    bulk_files.append(os.path.join(root, name))
                    visited_names.append(name)
                    # self._process_bulk_enumerate(bulk_files[-1])
                    # c+=1
                # if c==2:
                #     break
            # if c == 2:
            #     break


        pool = Pool(self.args.n_cpus)
        for out in tqdm(pool.imap_unordered(self._process_bulk_enumerate, bulk_files), total=len(bulk_files)):
            if out[0]: # is RS
                key = 'submissions'
            else:
                key = 'comments'

            if out[2] not in all_count[key].keys():
                all_count[key][out[2]] = out[1]
            else:
                all_count[key][out[2]] += out[1]

            all_count['explored'].append(out[3])
            with open('all_data_count.json', mode='w') as fW:
                json.dump(all_count, fW, indent=4)

        ###  writing the dictionay
        with open('all_data_count.json', mode='w') as fW:
            json.dump(all_count, fW, indent=4)
