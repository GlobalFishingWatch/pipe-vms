import apache_beam as beam
from vms_ingestion.normalization.feeds.bra_normalize import BRANormalize
from vms_ingestion.normalization.feeds.chl_normalize import CHLNormalize
from vms_ingestion.normalization.feeds.cri_normalize import CRINormalize
from vms_ingestion.normalization.feeds.ecu_normalize import ECUNormalize
from vms_ingestion.normalization.feeds.nor_normalize import NORNormalize
from vms_ingestion.normalization.feeds.per_normalize import PERNormalize


class FeedNormalizationFactory:

    @staticmethod
    def get_normalization(feed) -> beam.PTransform:
        feed_id = (feed or "").lower()
        if feed_id == "bra":
            return BRANormalize(feed=feed_id)
        elif feed_id == "chl":
            return CHLNormalize(feed=feed_id)
        elif feed_id == "cri":
            return CRINormalize(feed=feed_id)
        elif feed_id == "ecu":
            return ECUNormalize(feed=feed_id)
        elif feed_id == "nor":
            return NORNormalize(feed=feed_id)
        elif feed_id == "per":
            return PERNormalize(feed=feed_id)
        else:
            raise ValueError(feed)
