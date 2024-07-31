import apache_beam as beam
from vms_ingestion.normalization.transforms.map_normalized_message import (
    MapNormalizedMessage,
)
from vms_ingestion.normalization.transforms.nor_map_source_message import (
    NORMapSourceMessage,
)


class NORNormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = "FISKERIDIR"
        self.source_format = "NORWAY_VMS"

    def expand(self, pcoll):

        return (
            pcoll
            | NORMapSourceMessage()
            | MapNormalizedMessage(
                feed=self.feed,
                source_provider=self.source_provider,
                source_format=self.source_format,
            )
        )
