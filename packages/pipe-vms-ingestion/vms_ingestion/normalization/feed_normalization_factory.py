import apache_beam as beam
from vms_ingestion.normalization.feeds.bra_normalize import BRANormalize
from vms_ingestion.normalization.feeds.chl_normalize import CHLNormalize
from vms_ingestion.normalization.feeds.cri_normalize import CRINormalize
from vms_ingestion.normalization.feeds.ecu_normalize import ECUNormalize
from vms_ingestion.normalization.feeds.nor_normalize import NORNormalize
from vms_ingestion.normalization.feeds.pan_normalize import PANNormalize
from vms_ingestion.normalization.feeds.per_normalize import PERNormalize

NORMALIZER_BY_FEED = {
    "bra": BRANormalize,
    "chl": CHLNormalize,
    "cri": CRINormalize,
    "ecu": ECUNormalize,
    "nor": NORNormalize,
    "pan": PANNormalize,
    "per": PERNormalize,
}


class FeedNormalizationFactory:

    @staticmethod
    def get_normalization(feed) -> beam.PTransform:
        feed_id = (feed or "").lower()
        if feed_id in NORMALIZER_BY_FEED:
            normalizer = NORMALIZER_BY_FEED[feed_id]
            return normalizer(feed=feed_id)

        raise ValueError(feed)
