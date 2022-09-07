import glob
import json
import pickle
from functools import reduce
from multiprocessing import Pool
from os.path import join as pjoin

from tqdm import tqdm

from Reddit import Reddit


def reducer(accumulator, element):
    for key, value in element.items():

        accumulator[key] = accumulator.get(key, []) + value

    return accumulator

class RedditAggregator(Reddit):
    def __init__(self, args):
        super(RedditAggregator, self).__init__(args)
        self.args = args
        self.comment_dict = {}

    def _process_bulk(self, param):

        local_cm_dict = {}
        with open(param, mode='r') as rsFile:
            # num_lines = sum([1 for _ in open(param)])
            # for line in tqdm(rsFile, total=num_lines):
            for line in rsFile:
                reddit_post = json.loads(line)
                if len(reddit_post['body'].strip()) > 0 and 'tldr' in reddit_post['body'].strip().lower():
                    if reddit_post['link_id'] not in local_cm_dict.keys():
                        local_cm_dict[reddit_post['link_id']] = [reddit_post['body'].strip()]
                    else:
                        local_cm_dict[reddit_post['link_id']].append(reddit_post['body'].strip())

        return local_cm_dict



    def _run(self):
        """
            # reading bulk files from `args.base_dir` into a list
            # this list will then be used for multiprocessing
            # each CPU will process one bulk data, and after processing,
            # it will write a single file to the `args.write_dir` directory.
        """
        bulk_files = []
        for bulk_file in glob.glob(pjoin(self.args.read_dir, "RC*")):
            if '.' not in bulk_file:
                bulk_files.append(bulk_file)

        print(f'Processing {len(bulk_files)} bulk files started...')

        pool = Pool(self.args.n_cpus)
        for local_id_to_file_mapping in tqdm(pool.imap_unordered(self._process_bulk, bulk_files), total=len(bulk_files)):
            self.comment_dict = reduce(reducer, [self.comment_dict, local_id_to_file_mapping], {})

        pkl_file = open(self.args.write_dir, 'wb')
        pickle.dump(self.comment_dict, pkl_file)

        print('------------')
        print('ENDED!')
