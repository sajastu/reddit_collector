import argparse
from multiprocessing import cpu_count

from Reddit import Reddit

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-mode", choices=['preprocess', 'comment_agg'], default='preprocess', type=str, help='Data mode')
    parser.add_argument("-set", default='', type=str, help='Set name; if not specified, will get through all sets')

    """
        Processor-specific arguments
    """
    parser.add_argument("-read_dir", default='/Users/sotudehg/PycharmProjects/reddit_collector',
                        type=str, help='Bulk file\'s read dir')
    parser.add_argument("-write_dir", default='/home/code-base/user_space/packages/datasets/reddit_tifu_segmented-ext/',
                        type=str, help='Directory containing single files after pre-processing bulk data')
    parser.add_argument("-n_cpus", default=cpu_count(), type=int, help='Number of CPUs to be used for pre-processing; default: all CPUs')

    """
        Data specific argument(s)
    """
    parser.add_argument('-tldr_th', default=4, type=int, help='# word threshold above which instances will be sampled. ')
    parser.add_argument('-lower', action='store_true', default=False, help='Either lowercase the post and TL;DR or not?')

    args = parser.parse_args()
    rObj = Reddit(args)
    rObj._run()

