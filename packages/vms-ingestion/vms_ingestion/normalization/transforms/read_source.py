import apache_beam as beam
import datetime as dt

class ReadSource(beam.PTransform):
    def __init__(self, source_query_template, source_table, date_range, labels):
        self.source_query_template = source_query_template
        self.source_table = source_table
        self.start_date, self.end_date = date_range
        self.labels = labels

    def expand(self, pcoll):
        return (
            pcoll
            | self.read_source()
        )

    def read_source(self):
        query =  self.source_query_template.format(
            source_table=self.source_table,
            start_date=self.start_date.strftime("%Y-%m-%d"),
            end_date=self.end_date.strftime("%Y-%m-%d"),
        )
        return beam.io.ReadFromBigQuery(
            query = query,
            use_standard_sql=True,
            bigquery_job_labels = self.labels,
        )
