from datetime import datetime, timezone

import apache_beam as beam
from shipdataprocess.normalize import normalize_callsign, normalize_shipname
from shipdataprocess.standardize import standardize_imo, standardize_str
from vms_ingestion.normalization.transforms.calculate_msgid import get_message_id
from vms_ingestion.normalization.transforms.calculate_ssvid import get_ssvid


def get_ingested_at():
    return datetime.now(tz=timezone.utc)


def map_ingested_message(msg, feed, fleet):
    result = {
        **msg,
        "source_tenant": standardize_str(feed),
        "source_fleet": standardize_str(msg.get("fleet") or fleet),
        "beacon_number": msg.get("beacon_number"),
        "shipname": standardize_str(msg["shipname"]),
        "timestamp": msg["timestamp"],
        "callsign": standardize_str(msg["callsign"]) if msg.get("callsign") else None,
        "flag": msg.get("flag"),
        "imo": standardize_imo(msg.get("imo")),
        "lat": msg["lat"],
        "lon": msg["lon"],
        "speed": msg["speed"],
        "heading": msg["heading"],
        "mmsi": msg.get("mmsi"),
        "internal_id": (
            standardize_str(msg["internal_id"]) if msg.get("internal_id") else None
        ),
        "fishing_zone": msg.get("fishing_zone"),
        "fishing_gear": msg.get("fishing_gear"),
        "ingested_at": get_ingested_at(),
        "timestamp_date": datetime.date(msg["timestamp"]),
    }
    return result


class MapIngestedMessage(beam.PTransform):

    def __init__(self, feed, fleet):
        self.feed = feed
        self.fleet = fleet

    def expand(self, pcoll):
        return (
            pcoll
            | self.map_ingested_message()
            | self.calculate_ssvid()
            | self.calculate_message_id()
        )

    def map_ingested_message(self):
        return beam.Map(
            lambda msg: map_ingested_message(msg=msg, feed=self.feed, fleet=self.fleet)
        )

    def calculate_message_id(self):
        return beam.Map(
            lambda msg: {
                **msg,
                "msgid": get_message_id(
                    timestamp=msg["timestamp"],
                    lat=msg["lat"],
                    lon=msg["lon"],
                    ssvid=msg["ssvid"],
                    fleet=msg.get("fleet"),
                    speed=msg.get("speed"),
                    course=msg.get("course"),
                ),
            }
        )

    def calculate_ssvid(self):
        return beam.Map(
            lambda msg: {
                **msg,
                "ssvid": get_ssvid(
                    country=msg["source_tenant"],
                    internal_id=msg.get("internal_id"),
                    shipname=normalize_shipname(msg.get("shipname")),
                    callsign=normalize_callsign(msg.get("callsign")),
                    licence=msg.get("licence"),
                ),
            }
        )
