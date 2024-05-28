from vms_ingestion.normalization.feed_normalization_pipeline import FeedNormalizationPipeline


class CHLFeedPipeline(FeedNormalizationPipeline):

    def __init__(self, source, destination, start_date, end_date, labels) -> None:
        super().__init__(source, destination, start_date, end_date, labels, feed='chl')

