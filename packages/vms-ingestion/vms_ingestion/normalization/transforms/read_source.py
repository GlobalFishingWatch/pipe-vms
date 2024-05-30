import apache_beam as beam
from jinja2 import Environment, FileSystemLoader

templates = Environment(
    loader=FileSystemLoader(""),
)


class ReadSource(beam.PTransform):
    def __init__(self, source_query_template_path, source_table, date_range, labels):
        self.source_query_template_path = source_query_template_path
        self.source_table = source_table
        self.start_date, self.end_date = date_range
        self.labels = labels

    def expand(self, pcoll):
        return (
            pcoll
            | self.read_source()
        )

    def read_source(self):
        query_template = templates.get_template(self.source_query_template_path)
        query = query_template.render(
            source=self.source_table,
            start_date=self.start_date.strftime("%Y-%m-%d"),
            end_date=self.end_date.strftime("%Y-%m-%d"),
        )
        print(query)
        return beam.io.ReadFromBigQuery(
            query=query,
            use_standard_sql=True,
            bigquery_job_labels=self.labels,
        )
