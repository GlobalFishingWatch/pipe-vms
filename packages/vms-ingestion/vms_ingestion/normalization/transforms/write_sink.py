import apache_beam as beam


class WriteSink(beam.PTransform):
    def __init__(self,
                 table,
                 schema,
                 description,
                 write_disposition,
                 labels,
                 ):
        self.table = table
        self.write_disposition = write_disposition
        self.labels = labels

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )

    def write_sink(self):
        print(f'Writing Sink to {self.table}')
        return beam.io.WriteToBigQuery(
            table=self.table,
            write_disposition=self.write_disposition,
        )
