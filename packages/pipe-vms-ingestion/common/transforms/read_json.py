import apache_beam as beam
from common.coders.json_coder import JSONCoder
from common.transforms.decode_lines_with_custom_coder import DecodeLinesWithCustomCoder
from common.transforms.read_lines_from_file import ReadLinesFromFile


class ReadJson(beam.io.ReadFromText):
    """Read Json file and decode it"""

    def __init__(self, schema, error_topic):
        self.schema = schema
        self.error_topic = error_topic

    def expand(self, pcoll):
        def get_filename(element):
            bucket = element['bucket']
            filename = element['name']
            return f"{self.schema}{bucket}/{filename}"

        lines = pcoll | "Read file paths" >> beam.ParDo(ReadLinesFromFile(get_filename_fn=get_filename)).with_outputs(
            "errors", main="lines"
        )
        decoded_records = lines.lines | "Decode records" >> beam.ParDo(
            DecodeLinesWithCustomCoder(JSONCoder())
        ).with_outputs("errors", main="decoded")
        errors = (lines.errors, decoded_records.errors) | "Flatten errors" >> beam.Flatten()
        errors | "Write errors" >> beam.io.WriteToPubSub(topic=self.error_topic)

        return decoded_records.decoded
