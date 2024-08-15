import datetime as dt
from datetime import timezone

import apache_beam as beam
from shipdataprocess.standardize import standardize_str
from vms_ingestion.ingestion.excel_to_bq.transforms.map_ingested_message import (
    MapIngestedMessage,
)


# Function to convert DMS to decimal
def dms_to_decimal(dms_str):
    dms_str = dms_str.strip()
    degrees, rest = dms_str.split("°")
    minutes, rest = rest.split("'")
    seconds, direction = rest.split('" ')
    degrees = float(degrees)
    minutes = float(minutes)
    seconds = float(seconds)
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ["S", "W"]:
        decimal = -decimal
    return decimal


def extract_float(df_val):
    df_val = df_val.strip()
    if df_val == "-":
        return None
    value, _ = df_val.split(" ")
    return float(value)


def extract_int(df_val):
    df_val = df_val.strip()
    if df_val == "-":
        return None
    value, _ = df_val.split(" ")
    return int(value)


def map_pan_fields(msg):
    return {
        "shipname": standardize_str(msg.get("Nombre de la nave")),
        "lat": dms_to_decimal(msg.get("Latitud")),
        "lon": dms_to_decimal(msg.get("Longitud")),
        "speed": extract_float(msg.get("Velocidad")),
        "heading": extract_int(msg.get("Rumbo")),
        "timestamp": dt.datetime.strptime(
            msg.get("Fecha de la posición") + " " + msg.get("Hora de la posición"),
            "%d/%m/%Y %H:%M",
        ),
        "internal_id": standardize_str(msg.get("registry_number")),
    }


def get_ingested_at():
    return dt.datetime.now(tz=timezone.utc)


class PANIngest(beam.PTransform):

    def __init__(self, feed, fleet=None) -> None:
        self.feed = feed
        self.fleet = fleet

    def expand(self, pcoll):

        return (
            pcoll
            | "map position fields" >> beam.Map(lambda x: map_pan_fields(x))
            | "Map ingested message"
            >> MapIngestedMessage(feed=self.feed, fleet=self.fleet)
        )
