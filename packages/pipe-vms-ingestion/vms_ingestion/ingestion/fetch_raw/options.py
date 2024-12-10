from apache_beam.options.pipeline_options import PipelineOptions


class IngestionFetchRawOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        optional = parser.add_argument_group("Optional")

        optional.add_argument(
            "--input_topic",
            required=False,
            help="The Cloud Pub/Sub topic to read from.",
        )

        optional.add_argument(
            "--output_topic",
            required=False,
            help="The Cloud Pub/Sub topic to write positions to.",
        )

        optional.add_argument(
            "--error_topic",
            required=False,
            help="The Cloud Pub/Sub topic to write failed messages to.",
        )
