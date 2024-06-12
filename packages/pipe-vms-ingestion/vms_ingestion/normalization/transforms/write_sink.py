from datetime import datetime, timezone

import apache_beam as beam
from bigquery import query
from google.cloud import bigquery
from vms_ingestion import __getattr__


def table_description():
    return f"""
Created by pipe-vms-ingestion: {__getattr__('version')}.

* Normalized positions for all vms providers. Daily produced.
* https://github.com/GlobalFishingWatch/pipe-vms/packages/pipe-vms-ingestion

* Source: Multiple sources from the different countries feeds.
* Last Updated: {datetime.isoformat(datetime.now(tz=timezone.utc))}
"""


def table_schema():
    return query.get_schema('assets/feeds/normalized.schema.json')


def table_descriptor(destination, labels, schema=table_schema()):

    table = bigquery.Table(
        destination,
        schema=schema,
    )
    table.description = table_description()
    table.clustering_fields = ['source_tenant', 'timestamp_date']
    table.time_partitioning = bigquery.table.TimePartitioning(
        type_=bigquery.table.TimePartitioningType.MONTH,
        field='timestamp',
    )
    table.labels = labels
    return table


class WriteSink(beam.PTransform):
    def __init__(self,
                 destination,
                 labels,
                 ):
        self.table = table_descriptor(destination, labels)
        self.labels = labels

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )

    def write_sink(self):
        print(f'Writing Sink to {self.table.project}.{self.table.dataset_id}.{self.table.table_id}')
        return beam.io.WriteToBigQuery(
            table=f'{self.table.project}.{self.table.dataset_id}.{self.table.table_id}',
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
