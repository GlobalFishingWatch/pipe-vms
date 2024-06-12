# from datetime import datetime

import apache_beam as beam


def convert_speed_kph_to_kt(speed_kph):
    return float(speed_kph)/1.852


class ConvertSpeedKPHToKT(beam.PTransform):

    def expand(self, pcoll):
        return (
            pcoll
            | "Convert speed kph to kt" >> beam.Map(lambda msg: self.convert_speed_kph_to_kt(msg))
        )

    def convert_speed_kph_to_kt(self, msg):
        speed_kph = msg.pop("speed_kph")
        return dict(**msg, speed=convert_speed_kph_to_kt(speed_kph))
