from vms_ingestion.normalization.pipeline import NormalizationPipeline
from vms_ingestion.normalization.transforms.bra_map_source_message import \
    BRAMapSourceMessage
from vms_ingestion.normalization.transforms.convert_speed import \
    ConvertSpeedKPHToKT
from vms_ingestion.normalization.transforms.map_normalized_message import \
    MapNormalizedMessage
from vms_ingestion.normalization.transforms.read_source import ReadSource
from vms_ingestion.normalization.transforms.write_sink import WriteSink


class BRAFeedPipeline(NormalizationPipeline):

    def __init__(self,
                 options,
                 read_source=ReadSource,
                 write_sink=WriteSink) -> None:
        super().__init__(options)
        self.source_provider = 'ONYXSAT'
        self.source_format = 'ONYXSAT_BRAZIL_VMS'

        (
            self.pipeline
            | "Read source" >> read_source(source_table=self.source,
                                           source_timestamp_field=self.source_timestamp_field,
                                           date_range=(self.start_date, self.end_date),
                                           labels=self.labels)
            | BRAMapSourceMessage()
            | ConvertSpeedKPHToKT()
            | MapNormalizedMessage(feed=self.feed,
                                   source_provider=self.source_provider,
                                   source_format=self.source_format)
            | "Write Sink" >> write_sink(destination=self.destination,
                                         labels=self.labels,)
        )
