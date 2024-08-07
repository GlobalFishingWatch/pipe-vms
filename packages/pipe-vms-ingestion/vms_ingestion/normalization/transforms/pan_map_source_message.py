import apache_beam as beam
from shipdataprocess.normalize import normalize_shipname
from utils.convert import to_float


def pan_map_source_message(msg):
    return {
        "callsign": f'{msg["callsign"]}'.strip(),
        "course": to_float(msg["course"]),
        "imo": f'{msg["imo"]}'.strip(),
        "lat": to_float(msg["lat"]),
        "lon": to_float(msg["lon"]),
        "mmsi": f'{msg["mmsi"]}'.strip(),
        "shipname": f'{msg["shipname"]}'.strip(),
        "shiptype": f'{msg["shiptype"]}'.strip(),
        "speed": to_float(msg["speed"]),
        "ssvid": normalize_shipname(msg.get("shipname")),
        "timestamp": msg["timestamp"],
    }


class PANMapSourceMessage(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | "Preliminary source fields mapping" >> beam.Map(
            pan_map_source_message
        )
