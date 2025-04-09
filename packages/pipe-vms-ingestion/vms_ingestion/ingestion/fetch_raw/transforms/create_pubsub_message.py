import apache_beam as beam
from apache_beam.io import PubsubMessage


class CreatePubsubMessage(beam.DoFn):
    def process(self, element):
        yield PubsubMessage(data=element["data"], attributes=element["attributes"])
