import apache_beam as beam
from utils.convert import to_float, to_string

SHIPTYPE_BY_VESSEL_TYPE = {
    "FISKEFARTØY": "fishing",
    "FISKEFARTØY (AKTIV)": "fishing",
    "FORSKNINGSSKIP": "research",
    "TARETRÅLER": "kelp trawlers",
}


def nor_map_source_message(msg):
    return {
        "shipname": to_string(msg["vessel_name"]),
        "msgid": to_string(msg["message_id"]),
        "timestamp": msg["timestamp_utc"],
        "lat": to_float(msg["lat"]),
        "length": to_float(msg["length"]),
        "lon": to_float(msg["lon"]),
        "speed": to_float(msg["speed"]),
        "course": to_float(msg["course"]),
        "shiptype": nor_infer_shiptype(msg["vessel_type"]),
        "callsign": to_string(msg["callsign"]),
    }


def nor_infer_shiptype(vessel_type):
    if f"{vessel_type}".strip().upper() in SHIPTYPE_BY_VESSEL_TYPE:
        return SHIPTYPE_BY_VESSEL_TYPE[f"{vessel_type}".strip().upper()]

    return f"{vessel_type}".strip().lower()


class NORMapSourceMessage(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | "Preliminary source fields mapping" >> beam.Map(
            nor_map_source_message
        )
