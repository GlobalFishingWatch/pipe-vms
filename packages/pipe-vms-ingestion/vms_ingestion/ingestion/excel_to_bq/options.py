from apache_beam.options.pipeline_options import PipelineOptions
from common import validators


class IngestionExcelToBQOptions(PipelineOptions):
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
            "--fleet",
            required=True,
            help="Source GCS path to the folder containing the Excel files.",
        )

        required.add_argument(
            "--source",
            required=True,
            help="Source GCS path to the folder containing the Excel files.",
        )

        required.add_argument(
            "--destination",
            required=True,
            help="Destination table prefix to write messages to, in the standard sql format PROJECT.DATASET.TABLE.",
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
            "--wait_for_job",
            default=False,
            action="store_true",
            help="Wait until the job finishes before returning.",
        )
