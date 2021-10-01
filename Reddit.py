from file_downloader import RedditDownloader


class Reddit():
    def __init__(self, args):
        self.args = args

    def _run(self):
        eval(f'self.{self.args.mode}()')

    def download_and_unzip(self):
        from file_downloader import RedditDownloader
        rd = RedditDownloader(self.args)
        rd._run()

    def preprocess(self):
        from reddit_processor import RedditProcessor
        rp = RedditProcessor(self.args)
        rp._run()

    def comment_agg(self):
        from reddit_cm_aggregator import RedditAggregator
        rp = RedditAggregator(self.args)
        rp._run()
