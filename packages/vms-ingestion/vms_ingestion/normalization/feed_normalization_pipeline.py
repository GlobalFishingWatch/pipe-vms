from datetime import datetime, timezone

import apache_beam as beam
from bigquery import query
from bigquery.table import clear_records, ensure_table_exists
from google.cloud import bigquery
from vms_ingestion import __getattr__
from vms_ingestion.normalization.transforms.read_source import ReadSource
from vms_ingestion.normalization.transforms.write_sink import WriteSink


class FeedNormalizationPipeline():

    def __init__(self,
                 source,
                 destination,
                 start_date,
                 end_date,
                 labels,
                 feed=None,
                 ) -> None:
        super().__init__()
        self.feed = feed
        self.source = source
        self.destination = destination
        self.start_date = start_date
        self.end_date = end_date
        self.labels = labels
        self.__source_query_template = None
        self.__table_descriptor = None
        self.__table_schema = None

    @property
    def source_query_path(self):
        return f'assets/feeds/{self.feed}.source_query.sql.j2'

    @property
    def source_query_template(self):
        if not self.__source_query_template:
            with open(self.source_query_path) as f:
                self.__source_query_template = f.read()
        return self.__source_query_template

    @property
    def table_description(self):
        return f"""
Created by pipe-vms-normalization: {__getattr__('version')}.

* Normalized positions for all vms providers. Daily produced.
* https://github.com/GlobalFishingWatch/pipe-vms/packages/vms-ingestion

* Source: Multiple sources from the different countries feeds.
* Last Updated: {datetime.isoformat(datetime.now(tz=timezone.utc))}
"""

    @property
    def table_schema(self):
        if not self.__table_schema:
            self.__table_schema = query.get_schema(
                'assets/feeds/normalized.schema.json')
        return self.__table_schema

    @property
    def table_descriptor(self):
        if not self.__table_descriptor:
            table = bigquery.Table(
                self.destination,
                schema=self.table_schema,
            )
            table.description = self.table_description
            table.clustering_fields = ['source_tenant', 'timestamp_date']
            table.time_partitioning = bigquery.table.TimePartitioning(
                type_=bigquery.table.TimePartitioningType.MONTH,
                field='timestamp',
            )
            table.labels = self.labels
            self.__table_descriptor = table
        return self.__table_descriptor

    def ensure_table_exists(self):
        self.__table_descriptor = ensure_table_exists(self.table_descriptor)
        return self.__table_descriptor

    def clear_records(self):
        additional_conditions = [f'source_tenant = \'{self.feed}\'']
        return clear_records(table_id=self.destination,
                             date_field='timestamp',
                             date_from=self.start_date,
                             date_to=self.end_date,
                             additional_conditions=additional_conditions,
                             )

    def read_source(self):
        return ReadSource(source_query_template_path=self.source_query_path,
                          source_table=self.source,
                          date_range=(self.start_date, self.end_date),
                          labels=self.labels,
                          )

    def write_sink(self):
        table = self.table_descriptor
        return WriteSink(
            table=table.full_table_id,
            schema=table.schema,
            description=table.description,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            labels=self.labels,
        )

    def mapToNormalizedMessage(self, msg):
        return {
            "msgid": msg["msgid"],
            "source": msg["source"],
            "source_type": "VMS",
            "source_tenant": f'{self.feed.upper()}',
            "source_provider": msg.get("source_provider"),
            "source_ssvid": msg["ssvid"],
            "source_fleet": msg.get("fleet"),
            "type": msg.get("type", "VMS"),
            "ssvid": self.get_unique_ssvid(msg["ssvid"]),
            "timestamp": msg["timestamp"],
            "lat": msg["lat"],
            "lon": msg["lon"],
            "speed": msg.get("speed"),
            "course": msg.get("course"),
            "heading": msg.get("heading"),
            "shipname": msg["shipname"],
            "callsign": msg["callsign"],
            "destination": msg.get("destination"),
            "imo": msg.get("imo"),
            "shiptype": msg.get("shiptype"),
            "receiver_type": msg.get("receiver_type"),
            "receiver": msg.get("receiver"),
            "length": msg.get("length"),
            "width": msg.get("width"),
            "status": msg.get("status"),
            "class_b_cs_flag": msg.get('class_b_cs_flag'),
            "received_at": msg.get('received_at'),
            "ingested_at": msg.get('ingested_at'),
            "timestamp_date": datetime.date(msg["timestamp"]),
        }

    def get_unique_ssvid(self, ssvid):
        return f'{self.feed}|{ssvid}'

    class Normalize(beam.PTransform):
        def __init__(self, pipe, label=None) -> None:
            super().__init__(label)
            self.pipe = pipe

        def expand(self, pcoll):
            return (
                pcoll
                | beam.Map(self.pipe.mapToNormalizedMessage)
            )

    def normalize(self):
        return self.Normalize(pipe=self)
