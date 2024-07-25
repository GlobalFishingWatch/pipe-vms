import apache_beam as beam
from vms_ingestion.normalization.transforms.ecu_map_source_message import (
    ECUMapSourceMessage,
)
from vms_ingestion.normalization.transforms.map_normalized_message import (
    MapNormalizedMessage,
)


class ECUNormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = "DIRNEA"
        self.source_format = "ECUADOR_VMS"

    def expand(self, pcoll):

        return (
            pcoll
            | ECUMapSourceMessage()
            | MapNormalizedMessage(
                feed=self.feed,
                source_provider=self.source_provider,
                source_format=self.source_format,
            )
        )
