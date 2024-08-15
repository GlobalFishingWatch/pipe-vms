from vms_ingestion.options import CommonPipelineOptions


class IngestionExcelToBQOptions(CommonPipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        optional = parser.add_argument_group("Optional")

        optional.add_argument(
            "--fleet",
            required=False,
            help="Fleet where the vessel belongs to",
        )
