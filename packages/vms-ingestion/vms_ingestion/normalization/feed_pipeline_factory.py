from vms_ingestion.normalization import config
from vms_ingestion.normalization.feed_normalization_pipeline import \
    FeedNormalizationPipeline


class FeedPipelineFactory:

    @staticmethod
    def get_pipeline(feed) -> FeedNormalizationPipeline:
        feed_id = (feed or "").lower()
        if feed_id == 'chl':
            return config.CHLFeedPipeline
        else:
            raise ValueError(feed)
