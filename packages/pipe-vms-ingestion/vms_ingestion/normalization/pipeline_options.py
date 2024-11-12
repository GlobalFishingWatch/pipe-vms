import argparse
from enum import Enum

from vms_ingestion.options import CommonPipelineOptions


# class syntax
class Entities(Enum):
    POSITIONS = "positions"
    VESSEL_INFO = "vessel-info"


def validate_affected_entities(value):
    accepted_values = [Entities.POSITIONS, Entities.VESSEL_INFO]

    entities = value.split(",")
    if len(entities) == 0:
        raise argparse.ArgumentTypeError(
            "Invalid affected entities value. At least one entity must be provided"
        )

    for entity in entities:
        if entity not in accepted_values:
            raise argparse.ArgumentTypeError(
                f"Invalid entity name {entity}. Valid options are {accepted_values}"
            )

    return value


class NormalizationOptions(CommonPipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        optional = parser.add_argument_group("Optional")

        optional.add_argument(
            "--affected-entities",
            required=False,
            type=validate_affected_entities,
            default=[Entities.POSITIONS, Entities.VESSEL_INFO].join(","),
            help="Which entities will be stored by this pipeline",
        )
