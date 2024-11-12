import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from bigquery.table import clear_records, ensure_table_exists
from common.transforms.pick_output_fields import PickOutputFields
from utils.convert import list_to_dict
from utils.dates import parse_yyyy_mm_dd_param
from vms_ingestion.normalization.feed_normalization_factory import (
    FeedNormalizationFactory,
)
from vms_ingestion.normalization.pipeline_options import Entities, NormalizationOptions
from vms_ingestion.normalization.transforms.deduplicate_msgs import DeduplicateMsgs
from vms_ingestion.normalization.transforms.deduplicate_vessel_info import (
    DeduplicateVesselInfo,
)
from vms_ingestion.normalization.transforms.discard_zero_lat_lon import (
    DiscardZeroLatLon,
)
from vms_ingestion.normalization.transforms.filter_date_range import FilterDateRange
from vms_ingestion.normalization.transforms.map_latlon import MapLatLon
from vms_ingestion.normalization.transforms.read_source import ReadSource
from vms_ingestion.normalization.transforms.write_sink import (
    WriteSink,
    table_descriptor,
    table_schema,
)
from vms_ingestion.normalization.transforms.write_sink_reported_vessel_info import (
    WriteSinkReportedVesselInfo,
)
from vms_ingestion.normalization.transforms.write_sink_reported_vessel_info import (
    table_descriptor as table_descriptor_vessel_info,
)
from vms_ingestion.normalization.transforms.write_sink_reported_vessel_info import (
    table_schema as table_schema_vessel_info,
)


class NormalizationPipeline:
    def __init__(self, options):
        self.pipeline = beam.Pipeline(options=options)

        params = options.view_as(NormalizationOptions)
        gCloudParams = options.view_as(GoogleCloudOptions)

        self.feed = params.country_code
        self.source = params.source
        self.source_timestamp_field = params.source_timestamp_field
        self.destination = params.destination
        self.affected_entities = params.affected_entities.split(",")

        default_vessel_info_parts = self.destination.split(".")[:2] + [
            "reported_vessel_info"
        ]
        dst_vessel_info_parts = [
            x for x in params.destination_vessel_info.split(".") if x
        ]
        while len(dst_vessel_info_parts) < 3:
            index = len(default_vessel_info_parts) - len(dst_vessel_info_parts) - 1
            dst_vessel_info_parts.insert(0, default_vessel_info_parts[index])

        self.destination_vessel_info = ".".join(dst_vessel_info_parts)
        self.start_date = parse_yyyy_mm_dd_param(params.start_date)
        self.end_date = parse_yyyy_mm_dd_param(params.end_date)
        self.labels = list_to_dict(gCloudParams.labels)

        self.table_schema = table_schema()
        self.output_fields = [field["name"] for field in self.table_schema]

        self.table_schema_vessel_info = table_schema_vessel_info()
        self.output_fields_vessel_info = [
            field["name"] for field in self.table_schema_vessel_info
        ]

        if self.destination:
            if Entities.POSITIONS in self.affected_entities:
                # Ensure output tables exists
                ensure_table_exists(
                    table=table_descriptor(
                        destination=self.destination,
                        labels=self.labels,
                        schema=self.table_schema,
                    )
                )
                # Clear records on the given period and country (feed)
                clear_records(
                    table_id=self.destination,
                    date_field="timestamp",
                    date_from=self.start_date,
                    date_to=self.end_date,
                    additional_conditions=[
                        f"upper(source_tenant) = upper('{self.feed}')"
                    ],
                )
            if Entities.VESSEL_INFO in self.affected_entities:
                # Ensure output tables exists
                ensure_table_exists(
                    table=table_descriptor_vessel_info(
                        destination=self.destination_vessel_info,
                        labels=self.labels,
                        schema=self.table_schema_vessel_info,
                    )
                )
                # Clear records on the given period and country (feed)
                clear_records(
                    table_id=self.destination_vessel_info,
                    date_field="timestamp",
                    date_from=self.start_date,
                    date_to=self.end_date,
                    additional_conditions=[
                        f"upper(source_tenant) = upper('{self.feed}')"
                    ],
                )

        position_messages = (
            self.pipeline
            | "Read source"
            >> ReadSource(
                source_table=self.source,
                source_timestamp_field=self.source_timestamp_field,
                date_range=(self.start_date, self.end_date),
                labels=self.labels,
            )
            | "Map Lat/Lon alternative field names" >> MapLatLon()
            | "Discard Zero Lat and Lon" >> DiscardZeroLatLon()
            | "Normalize" >> FeedNormalizationFactory.get_normalization(feed=self.feed)
            | "Deduplicate" >> DeduplicateMsgs()
            | "Filter date range"
            >> FilterDateRange(date_range=(self.start_date, self.end_date))
        )

        if Entities.POSITIONS in self.affected_entities:
            # Store normalized positions
            (
                position_messages
                | PickOutputFields(fields=[f"{field}" for field in self.output_fields])
                | "Write Sink"
                >> WriteSink(
                    destination=self.destination,
                    labels=self.labels,
                )
            )
        if Entities.VESSEL_INFO in self.affected_entities:
            # Store reported vessel info
            (
                position_messages
                | "Deduplicate Vessel Info" >> DeduplicateVesselInfo()
                | "Pick Output Fields Vessel Info"
                >> PickOutputFields(
                    fields=[f"{field}" for field in self.output_fields_vessel_info]
                )
                | "Write Sink Vessel Info"
                >> WriteSinkReportedVesselInfo(
                    destination=self.destination_vessel_info,
                    labels=self.labels,
                )
            )

    def run(self):
        return self.pipeline.run()
