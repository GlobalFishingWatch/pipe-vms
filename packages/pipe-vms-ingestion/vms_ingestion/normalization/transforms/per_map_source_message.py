import datetime as dt

import apache_beam as beam
from utils.convert import to_float, to_string

FLEET_BY_REGIMEN_DESCRIPTION = {
    "ARTESANAL": "artisanal",
    "DECRETO LEGISLATIVO N° 1392": "artisanal",
    "DECRETO LEGISLATIVO N 1392": "artisanal",  # changed name
    "DECRETO LEGISLATIVO Nº1273": "artisanal",
    "DECRETO LEGISLATIVO N° 1273": "artisanal",  # changed name
    "DECRETO LEGISLATIVO  N 1273": "artisanal",  # changed name
    "DS. 006-2016-PRODUCE": "artisanal",
    "NO ESPECIFICADO": "artisanal",
    "MENOR ESCALA": "small-scale",
    "MENOR ESCALA (ANCHOVETA) - ARTESANAL": "small-scale",
    "DS. 020-2011-PRODUCE": "small-scale",
    "MENOR ESCALA (ANCHOVETA)": "small-scale",
    "DECRETO LEY Nº25977": "industrial",
    "DS. 022-2009-PRODUCE": "industrial",
    "D.S 022-2009-PRODUCE": "industrial",  # changed name
    "DS. 032-2003-PRODUCE": "industrial",
    "DS. 016-2020-PRODUCE": "industrial",  # new category
    "LEY Nº26920": "industrial",
}


def per_map_source_message(msg):
    return {
        "shipname": to_string(msg["nickname"]),
        # convert timestamp from peru timezone to utc
        "timestamp": msg["DATETRANSMISSION"] + dt.timedelta(hours=5),
        "fleet": per_infer_fleet(msg["DESC_REGIMEN"]),
        "lat": to_float(msg["LATITUDE"]),
        "lon": to_float(msg["LONGITUDE"]),
        "speed": to_float(msg["SPEED"]),
        "course": to_float(msg["COURSE"]),
        "ssvid": to_string(msg["PLATE"]),
        "callsign": None,
    }


def per_infer_fleet(regime_description):
    if f"{regime_description}".strip().upper() in FLEET_BY_REGIMEN_DESCRIPTION:
        return FLEET_BY_REGIMEN_DESCRIPTION[f"{regime_description}".strip().upper()]

    return "not defined"


class PERMapSourceMessage(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | "Preliminary source fields mapping" >> beam.Map(
            per_map_source_message
        )
