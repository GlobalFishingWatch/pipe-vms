import apache_beam as beam
from jinja2 import BaseLoader, Environment

templates = Environment(
    loader=BaseLoader,
)

SOURCE_QUERY_TEMPLATE = """
    SELECT
      DISTINCT *
    FROM
      `{{source_table}}`
    WHERE
      DATE({{source_timestamp_field}}) >= '{{start_date}}'
      AND DATE({{source_timestamp_field}}) < '{{end_date}}'
"""


class ReadSource(beam.PTransform):
    def __init__(self, source_table, source_timestamp_field, date_range, labels):
        self.source_timestamp_field = source_timestamp_field
        self.source_table = source_table
        self.start_date, self.end_date = date_range
        self.labels = labels

    def expand(self, pcoll):
        return pcoll | self.read_source()

    def read_source(self):
        query_template = templates.from_string(SOURCE_QUERY_TEMPLATE)
        query = query_template.render(
            source_table=self.source_table,
            source_timestamp_field=self.source_timestamp_field,
            start_date=self.start_date.strftime("%Y-%m-%d"),
            end_date=self.end_date.strftime("%Y-%m-%d"),
        )
        return beam.io.ReadFromBigQuery(
            query=query,
            use_standard_sql=True,
            bigquery_job_labels=self.labels,
        )
