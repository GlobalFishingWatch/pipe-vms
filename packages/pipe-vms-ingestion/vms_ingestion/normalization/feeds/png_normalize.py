import apache_beam as beam
from vms_ingestion.normalization.transforms.map_normalized_message import (
    MapNormalizedMessage,
)
from vms_ingestion.normalization.transforms.png_map_source_message import (
    PNGMapSourceMessage,
)


class PNGNormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = "IFIMS"
        self.source_format = "PNG_VMS"

    def expand(self, pcoll):

        return (
            pcoll
            | PNGMapSourceMessage()
            | MapNormalizedMessage(
                feed=self.feed,
                source_provider=self.source_provider,
                source_format=self.source_format,
            )
        )
