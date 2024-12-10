# from typing import Any

import apache_beam as beam
from common.coders.naf_coder import NAFCoder
from common.transforms.read_lines_from_file import ReadLinesFromFile


class DecodeLinesWithCustomCoder(beam.DoFn):
    def __init__(self, coder):
        self.coder = coder

    def process(self, element):
        line = element['line']
        common = element['common']

        # Use the custom coder to decode lines
        try:
            yield {
                "message": self.coder.decode(line),
                "common": common,
            }
        except Exception as e:
            yield beam.pvalue.TaggedOutput("errors", {"line": line, "common": common, "error": str(e)})


class ReadNAF(beam.io.ReadFromText):
    """Read NAF file and decode it"""

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
            DecodeLinesWithCustomCoder(NAFCoder())
        ).with_outputs("errors", main="decoded")
        errors = (lines.errors, decoded_records.errors) | "Flatten errors" >> beam.Flatten()
        errors | "Write errors" >> beam.io.WriteToPubSub(topic=self.error_topic)

        return decoded_records.decoded
