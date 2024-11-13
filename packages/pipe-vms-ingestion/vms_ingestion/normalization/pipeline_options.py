import argparse
from enum import Enum

from vms_ingestion.options import CommonPipelineOptions


# class syntax
class Entities(str, Enum):
    POSITIONS = "positions"
    VESSEL_INFO = "vessel_info"


def validate_affected_entities(value):
    accepted_values = [Entities.POSITIONS, Entities.VESSEL_INFO]

    entities = value.split(",") if type(value) == str else []
    if len(entities) == 0:
        raise argparse.ArgumentTypeError(
            "Invalid affected entities value. At least one entity must be provided"
        )

    for entity in entities:
        if entity not in accepted_values:
            quoted_accepted_values = [f'"{v}"' for v in accepted_values]
            raise argparse.ArgumentTypeError(
                f'Invalid entity name "{entity}". Valid options are {", ".join(quoted_accepted_values)}'
            )

    return value


class NormalizationOptions(CommonPipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser):
        optional = parser.add_argument_group("Optional")
        optional.add_argument(
            "--destination_vessel_info",
            required=False,
            help="Destination table to write vessel info to, in the standard sql format PROJECT.DATASET.TABLE. \n"
            "Defaults to the same project and dataset provided in [destination] with reported_vessel_info "
            "table name.",
            default="reported_vessel_info",
        )

        optional.add_argument(
            "--affected_entities",
            required=False,
            type=validate_affected_entities,
            default=",".join([Entities.POSITIONS, Entities.VESSEL_INFO]),
            help="Which entities will be stored by this pipeline",
        )
