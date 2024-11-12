from apache_beam.options.pipeline_options import PipelineOptions
from common import validators


class CommonPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        required = parser.add_argument_group("Required")
        required.add_argument(
            "--country_code",
            required=True,
            type=validators.validate_country_name,
            help="Country to normalize raw data.",
        )

        required.add_argument(
            "--source",
            required=True,
            help="Source table to read messages from, in the standard sql format PROJECT.DATASET.TABLE. Usually, "
            + "this is the pre-thinned and filtered gfw_research.pipe_vXYZ table, such as gfw_research.pipe_v20201001.",
        )

        required.add_argument(
            "--destination",
            required=True,
            help="Destination table to write messages to, in the standard sql format PROJECT.DATASET.TABLE",
        )

        required.add_argument(
            "--start_date",
            required=True,
            help="Read the source table for messages after start date in format YYYY-MM-DD",
        )

        required.add_argument(
            "--end_date",
            required=True,
            help="Read the source table for messages before end date in format YYYY-MM-DD",
        )

        optional = parser.add_argument_group("Optional")
        optional.add_argument(
            "--source_timestamp_field",
            required=False,
            default="timestamp",
            help="Field name in source table that contains the timestamp of the position record.",
        )
        optional.add_argument(
            "--wait_for_job",
            default=False,
            action="store_true",
            help="Wait until the job finishes before returning.",
        )
