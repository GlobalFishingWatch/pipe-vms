import datetime as dt

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from bigquery.table import clear_records, ensure_table_exists
from vms_ingestion.normalization.feed_normalization_factory import (
    FeedNormalizationFactory,
)
from vms_ingestion.normalization.options import NormalizationOptions
from vms_ingestion.normalization.transforms.deduplicate_msgs import DeduplicateMsgs
from vms_ingestion.normalization.transforms.discard_zero_lat_lon import (
    DiscardZeroLatLon,
)
from vms_ingestion.normalization.transforms.pick_output_fields import PickOutputFields
from vms_ingestion.normalization.transforms.read_source import ReadSource
from vms_ingestion.normalization.transforms.write_sink import (
    WriteSink,
    table_descriptor,
    table_schema,
)


def parse_yyyy_mm_dd_param(value):
    return dt.datetime.strptime(value, "%Y-%m-%d")


def list_to_dict(labels):
    return {x.split("=")[0]: x.split("=")[1] for x in labels}


class NormalizationPipeline:
    def __init__(self, options):
        self.pipeline = beam.Pipeline(options=options)

        params = options.view_as(NormalizationOptions)
        gCloudParams = options.view_as(GoogleCloudOptions)

        self.feed = params.country_code
        self.source = params.source
        self.source_timestamp_field = params.source_timestamp_field
        self.destination = params.destination
        self.start_date = parse_yyyy_mm_dd_param(params.start_date)
        self.end_date = parse_yyyy_mm_dd_param(params.end_date)
        self.labels = list_to_dict(gCloudParams.labels)

        self.table_schema = table_schema()
        self.output_fields = [field["name"] for field in self.table_schema]

        if self.destination:
            # Ensure output table exists
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
                additional_conditions=[f"upper(source_tenant) = upper('{self.feed}')"],
            )

        (
            self.pipeline
            | "Read source"
            >> ReadSource(
                source_table=self.source,
                source_timestamp_field=self.source_timestamp_field,
                date_range=(self.start_date, self.end_date),
                labels=self.labels,
            )
            | "Discard Zero Lat and Lon" >> DiscardZeroLatLon()
            | "Normalize" >> FeedNormalizationFactory.get_normalization(feed=self.feed)
            | "Deduplicate" >> DeduplicateMsgs()
            | PickOutputFields(fields=[f"{field}" for field in self.output_fields])
            | "Write Sink"
            >> WriteSink(
                destination=self.destination,
                labels=self.labels,
            )
        )

    def run(self):
        return self.pipeline.run()
