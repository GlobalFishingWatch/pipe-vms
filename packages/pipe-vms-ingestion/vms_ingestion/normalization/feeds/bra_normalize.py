import apache_beam as beam
from vms_ingestion.normalization.transforms.bra_map_source_message import (
    BRAMapSourceMessage,
)
from vms_ingestion.normalization.transforms.convert_speed import ConvertSpeedKPHToKT
from vms_ingestion.normalization.transforms.map_normalized_message import (
    MapNormalizedMessage,
)


class BRANormalize(beam.PTransform):

    def __init__(self, feed) -> None:
        self.feed = feed
        self.source_provider = "ONYXSAT"
        self.source_format = "ONYXSAT_BRAZIL_VMS"

    def expand(self, pcoll):

        return (
            pcoll
            | BRAMapSourceMessage()
            | ConvertSpeedKPHToKT()
            | MapNormalizedMessage(
                feed=self.feed, source_provider=self.source_provider, source_format=self.source_format
            )
        )
