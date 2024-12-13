import apache_beam as beam


class DecodeLinesWithCustomCoder(beam.DoFn):
    def __init__(self, coder: beam.coders.Coder):
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
