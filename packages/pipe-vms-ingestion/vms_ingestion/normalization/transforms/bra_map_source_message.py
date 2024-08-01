# from datetime import datetime

import re

import apache_beam as beam
from utils.convert import to_float


def bra_map_source_message(msg):
    return {
        "shipname": f'{msg["nome"]}'.strip(),
        "timestamp": msg["datahora"],
        "lat": to_float(msg["lat"].replace(",", ".")),
        "lon": to_float(msg["lon"].replace(",", ".")),
        "speed_kph": to_float(msg["speed"]),
        "course": to_float(msg["curso"]),
        "internal_id": f'{msg["ID"]}' if msg.get("ID") else None,
        "msgid": f'{msg["mID"]}' if msg.get("mID") else None,
        "shiptype": bra_infer_shiptype(msg["codMarinha"]),
        "callsign": "",
    }


def bra_infer_shiptype(cod_marinha):
    # This code set the type fishing for codeMarinha that starts with a
    # number, the other: empty, CABOTAGEM, INDEFINIDO, PASSEIO, PESCA VOLUNT,
    # REBOCADOR, SERVICO, SERVIÇO, TESTE, UNDEFINED are left with no type.
    p = re.compile("^[A-Z|Ç]+|[ ]*$")
    code = f"{cod_marinha}".upper().strip()

    m = p.match(code)
    if m:
        return ""

    return "fishing"


class BRAMapSourceMessage(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | "Preliminary source fields mapping" >> beam.Map(
            bra_map_source_message
        )
