import apache_beam as beam
from apache_beam.io.filesystems import FileSystems


class ReadLinesFromFile(beam.DoFn):
    def __init__(self, get_filename_fn):
        self.get_filename_fn = get_filename_fn

    def process(self, element):
        filename = self.get_filename_fn(element)

        try:
            with FileSystems.open(filename) as f:
                for line in f:
                    yield {"line": line, "common": element["common"]}  # Each line is returned as a byte string
        except Exception as e:
            yield beam.pvalue.TaggedOutput("errors", {"path": filename, "error": str(e)})
