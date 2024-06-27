import apache_beam as beam
from vms_ingestion.normalization.transforms.map_normalized_message import MapNormalizedMessage


class CHLNormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = "SERNAPESCA"
        self.source_format = "chile_vms_{source_fleet}"

    def expand(self, pcoll):

        return pcoll | MapNormalizedMessage(
            feed=self.feed, source_provider=self.source_provider, source_format=self.source_format
        )
