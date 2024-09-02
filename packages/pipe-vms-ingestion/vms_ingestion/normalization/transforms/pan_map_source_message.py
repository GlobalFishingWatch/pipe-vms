import apache_beam as beam
from shipdataprocess.normalize import normalize_shipname
from utils.convert import to_float, to_string


def pan_map_source_message(msg):
    return {
        "callsign": to_string(msg["callsign"]),
        "course": to_float(msg["course"]),
        "imo": to_string(msg["imo"]),
        "lat": to_float(msg["lat"]),
        "lon": to_float(msg["lon"]),
        "mmsi": to_string(msg["mmsi"]),
        "shipname": to_string(msg["shipname"]),
        "shiptype": to_string(msg["shiptype"]),
        "speed": to_float(msg["speed"]),
        "ssvid": normalize_shipname(msg.get("shipname")),
        "timestamp": msg["timestamp"],
    }


class PANMapSourceMessage(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | "Preliminary source fields mapping" >> beam.Map(
            pan_map_source_message
        )
