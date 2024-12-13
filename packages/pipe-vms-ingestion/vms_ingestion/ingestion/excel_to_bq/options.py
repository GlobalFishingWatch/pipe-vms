from vms_ingestion.options import CommonPipelineOptions


class IngestionExcelToBQOptions(CommonPipelineOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.optional.add_argument(
            "--fleet",
            required=False,
            help="Fleet where the vessel belongs to",
        )
