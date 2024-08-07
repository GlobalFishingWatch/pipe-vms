import apache_beam as beam
from vms_ingestion.normalization.transforms.map_normalized_message import (
    MapNormalizedMessage,
)
from vms_ingestion.normalization.transforms.pan_map_source_message import (
    PANMapSourceMessage,
)


class PANNormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = "ARAP"
        self.source_format = "PANAMA_VMS"

    def expand(self, pcoll):

        return (
            pcoll
            | PANMapSourceMessage()
            | MapNormalizedMessage(
                feed=self.feed,
                source_provider=self.source_provider,
                source_format=self.source_format,
            )
        )
