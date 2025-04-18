# from datetime import datetime

import re

import apache_beam as beam
from utils.convert import to_float, to_string

SHIPTYPE_BY_MATRICULA = {
    "TI": "international traffic",
    "TN": "national traffic",
    "P": "fishing",
    "R": "tug",
    "B": "boat",
    "DA": "auxiliary",
}


def ecu_map_source_message(msg):
    return {
        "shipname": to_string(msg["nombrenave"]),
        "timestamp": msg["utc_time"],
        "lat": to_float(msg["lat"]),
        "lon": to_float(msg["lon"]),
        "speed": to_float(msg["velocidad"]),
        "course": to_float(msg["rumbo"]),
        "internal_id": to_string(msg["idnave"]),
        "shiptype": ecu_infer_shiptype(msg["matriculanave"]),
        "callsign": to_string(msg["matriculanave"]),
        "registry_number": to_string(msg["matriculanave"]),
    }


def ecu_infer_shiptype(matriculanave):
    # This code set the type fishing for matriculanave that starts with a
    # set of specific strings
    prefixes = "|".join(list(SHIPTYPE_BY_MATRICULA.keys()))
    p = re.compile(f"^({prefixes}).*$")
    code = f"{matriculanave or ''}".upper().strip()

    m = p.match(code)
    if m:
        return SHIPTYPE_BY_MATRICULA[m.group(1)]

    return "unknown"


class ECUMapSourceMessage(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | "Preliminary source fields mapping" >> beam.Map(
            ecu_map_source_message
        )
