import argparse

from Reddit import Reddit
from multiprocessing import cpu_count

if __name__ == '__main__':

    """
    
e.g.,
python main.py -mode preprocess -read_dir /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/submissions2021/ -write_dir /mnt/ilcompn0d1/user/sotudehg0/tldr_data_m2021-2 -comments_file /mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldr_post_comments/mapping_posts_comments.pk
python main.py -mode preprocess -read_dir /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/ -write_dir /home/sotudehg0/tldr_data_comments_full/ -comments_file /mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldr_post_comments/mapping_posts_comments.pk
python main.py -mode comment_agg -read_dir /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/ -write_dir /mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldr_post_comments/mapping_posts_comments.pk
        
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-mode", choices=['download_and_unzip','preprocess', 'comment_agg'], default='preprocess', type=str, help='Data mode')
    parser.add_argument("-base_dir", default='/home/code-base/user_space/packages/datasets/reddit_tifu_segmented/',
                        type=str, help='Dataset\'s base dir')
    parser.add_argument("-set", default='', type=str, help='Set name; if not specified, will get through all sets')


    """
        Processor-specific arguments
    """
    parser.add_argument("-read_dir", default='/Users/sotudehg/PycharmProjects/reddit_collector',
                        type=str, help='Bulk file\'s read dir')
    parser.add_argument("-write_dir", default='/home/code-base/user_space/packages/datasets/reddit_tifu_segmented-ext/',
                        type=str, help='Directory containing single files after pre-processing bulk data')
    parser.add_argument("-search_comments", default=False, type=bool,
                        help='Whether to search comments for finding tldr or not; default: True')
    parser.add_argument("-n_cpus", default=cpu_count(), type=int, help='Number of CPUs to be used for pre-processing; default: all CPUs')
    parser.add_argument("-comments_file", default='/mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldr_post_comments/mapping_posts_comments.pk',
                        type=str, help='Number of CPUs to be used for pre-processing; default: all GPUs')

    args = parser.parse_args()
    rObj = Reddit(args)
    rObj._run()

