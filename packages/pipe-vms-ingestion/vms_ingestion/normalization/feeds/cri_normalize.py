import apache_beam as beam
from vms_ingestion.normalization.transforms.map_normalized_message import \
    MapNormalizedMessage


class CRINormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = 'INCOPESCA'
        self.source_format = 'costarica_vms_{source_fleet}'

    def expand(self, pcoll):

        return (
            pcoll
            | MapNormalizedMessage(feed=self.feed,
                                   source_provider=self.source_provider,
                                   source_format=self.source_format)
        )