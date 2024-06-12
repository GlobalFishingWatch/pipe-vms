from vms_ingestion.normalization.feeds.bra_pipeline import BRAFeedPipeline
from vms_ingestion.normalization.feeds.chl_pipeline import CHLFeedPipeline
from vms_ingestion.normalization.feeds.cri_pipeline import CRIFeedPipeline
from vms_ingestion.normalization.pipeline import NormalizationPipeline


class FeedPipelineFactory:

    @staticmethod
    def get_pipeline(feed) -> NormalizationPipeline:
        feed_id = (feed or "").lower()
        if feed_id == 'bra':
            return BRAFeedPipeline
        elif feed_id == 'chl':
            return CHLFeedPipeline
        elif feed_id == 'cri':
            return CRIFeedPipeline
        else:
            raise ValueError(feed)
