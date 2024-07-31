import apache_beam as beam

SHIPTYPE_BY_VESSEL_TYPE = {
    "FISKEFARTØY": "fishing",
    "FISKEFARTØY (AKTIV)": "fishing",
    "FORSKNINGSSKIP": "research",
    "TARETRÅLER": "kelp trawlers",
}


def nor_map_source_message(msg):
    return {
        "shipname": f'{msg["vessel_name"]}'.strip(),
        "msgid": f'{msg["message_id"]}'.strip(),
        "timestamp": msg["timestamp_utc"],
        "lat": float(msg["lat"]),
        "length": float(msg["length"]),
        "lon": float(msg["lon"]),
        "speed": float(msg["speed"]) if msg.get("speed") is not None else None,
        "course": float(msg["course"]) if msg.get("course") is not None else None,
        "shiptype": nor_infer_shiptype(msg["vessel_type"]),
        "callsign": f'{msg["callsign"]}',
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
