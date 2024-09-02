import apache_beam as beam
from vms_ingestion.normalization.transforms.blz_map_source_message import (
    BLZMapSourceMessage,
)
from vms_ingestion.normalization.transforms.map_normalized_message import (
    MapNormalizedMessage,
)


class BLZNormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = "POLESTAR"
        self.source_format = "BELIZE_VMS"

    def expand(self, pcoll):

        return (
            pcoll
            | BLZMapSourceMessage()
            | MapNormalizedMessage(
                feed=self.feed,
                source_provider=self.source_provider,
                source_format=self.source_format,
            )
        )
