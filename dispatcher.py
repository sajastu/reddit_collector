
class Dispatcher():
    def __init__(self, args):
        self.args = args

    def _run(self):
        eval(f'self.{self.args.mode}()')

    def preprocess(self):
        from reddit_processor import RedditProcessor
        rp = RedditProcessor(self.args)
        rp._run()

    def comment_agg(self):
        from reddit_cm_aggregator import RedditAggregator
        rp = RedditAggregator(self.args)
        rp._run()
