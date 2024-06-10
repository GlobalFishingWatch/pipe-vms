from vms_ingestion.normalization.feed_normalization_pipeline import \
    FeedNormalizationPipeline
from vms_ingestion.normalization.feeds.chl_pipeline import CHLFeedPipeline


class FeedPipelineFactory:

    @staticmethod
    def get_pipeline(feed) -> FeedNormalizationPipeline:
        feed_id = (feed or "").lower()
        if feed_id == 'chl':
            return CHLFeedPipeline
        else:
            raise ValueError(feed)
