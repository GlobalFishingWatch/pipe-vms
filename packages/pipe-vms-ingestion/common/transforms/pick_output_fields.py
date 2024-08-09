import apache_beam as beam


def pick_output_fields(fields, msg):
    return {key: msg[key] for key in fields}


class PickOutputFields(beam.PTransform):

    def __init__(self, fields: list, label=None) -> None:
        super().__init__(label)
        self.fields = fields

    def expand(self, pcoll):
        return pcoll | self.pick_output_fields()

    def pick_output_fields(self):
        return beam.Map(lambda msg: pick_output_fields(fields=self.fields, msg=msg))
