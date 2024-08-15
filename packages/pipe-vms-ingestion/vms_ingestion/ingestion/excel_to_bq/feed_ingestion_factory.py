import apache_beam as beam
from vms_ingestion.ingestion.excel_to_bq.feeds.pan_ingest import PANIngest

INGESTOR_BY_FEED = {
    "pan": PANIngest,
}


class FeedIngestionFactory:

    @staticmethod
    def get_ingestion(feed, **kwargs) -> beam.PTransform:
        feed_id = (feed or "").lower()
        if feed_id in INGESTOR_BY_FEED:
            ingestor = INGESTOR_BY_FEED[feed_id]
            return ingestor(feed=feed_id, **kwargs)

        raise ValueError(feed)
