# from datetime import datetime

import apache_beam as beam


def bra_map_source_message(msg):
    return {"shipname": f'{msg["nome"]}'.strip(),
            "timestamp": msg["datahora"],
            "lat": float(msg["lat"].replace(",", ".")),
            "lon": float(msg["lon"].replace(",", ".")),
            "speed_kph": float(msg["speed"].replace(",", ".")) if msg.get("speed") else None,
            "course": float(msg["curso"].replace(",", ".")) if msg.get("curso") else None,
            "internal_id": f'{msg["ID"]}' if msg.get("ID") else None,
            "msgid": f'{msg["mID"]}' if msg.get("mID") else None,
            "callsign": None,
            }


class BRAMapSourceMessage(beam.PTransform):

    def expand(self, pcoll):
        return (
            pcoll
            | "Preliminary source fields mapping" >> beam.Map(bra_map_source_message)
        )
