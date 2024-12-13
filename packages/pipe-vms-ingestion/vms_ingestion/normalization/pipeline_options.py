from enum import Enum

from vms_ingestion.options import CommonPipelineOptions


class Entities(str, Enum):
    POSITIONS = "positions"
    VESSEL_INFO = "vessel_info"


class NormalizationOptions(CommonPipelineOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.optional.add_argument(
            "--destination_vessel_info",
            required=False,
            help="Destination table to write vessel info to, in the standard sql format PROJECT.DATASET.TABLE",
        )
