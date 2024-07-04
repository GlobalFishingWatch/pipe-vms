from datetime import datetime

import apache_beam as beam
from shipdataprocess.standardize import (
    standardize_imo,
    standardize_int_str,
    standardize_str,
)
from vms_ingestion.normalization.transforms.calculate_msgid import get_message_id
from vms_ingestion.normalization.transforms.calculate_ssvid import encode_ssvid


def map_normalized_message(msg, feed, source_provider, source_format):
    result = {
        **msg,
        "source_type": "VMS",
        "source_tenant": standardize_str(feed),
        "source_provider": standardize_str(source_provider),
        "source_fleet": standardize_str(msg.get("fleet")),
        "source_ssvid": msg.get("internal_id"),
        "type": standardize_str(msg.get("type", "VMS")),
        "timestamp": msg["timestamp"],
        "lat": msg["lat"],
        "lon": msg["lon"],
        "speed": msg.get("speed"),
        "course": msg.get("course"),
        "heading": msg.get("heading"),
        "shipname": standardize_str(msg["shipname"]),
        "callsign": standardize_str(msg["callsign"]) if msg["callsign"] else None,
        "destination": msg.get("destination"),
        "imo": standardize_imo(msg.get("imo")),
        "shiptype": standardize_str(msg.get("shiptype")),
        "receiver_type": msg.get("receiver_type"),
        "receiver": msg.get("receiver"),
        "length": msg.get("length"),
        "width": msg.get("width"),
        "status": standardize_int_str(msg.get("status")),
        "class_b_cs_flag": standardize_int_str(msg.get("class_b_cs_flag")),
        "received_at": msg.get("received_at"),
        "ingested_at": msg.get("ingested_at"),
        "timestamp_date": datetime.date(msg["timestamp"]),
    }
    return {**result, "source": standardize_str(source_format.format(**result))}


class MapNormalizedMessage(beam.PTransform):

    def __init__(self, feed, source_provider, source_format):
        self.feed = feed
        self.source_provider = source_provider
        self.source_format = source_format

    def expand(self, pcoll):
        return (
            pcoll
            | self.map_normalized_message()
            | self.calculate_ssvid()
            | self.calculate_message_id()
        )

    def map_normalized_message(self):
        return beam.Map(
            lambda msg: map_normalized_message(
                msg=msg,
                feed=self.feed,
                source_provider=self.source_provider,
                source_format=self.source_format,
            )
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
                "ssvid": encode_ssvid(
                    country=msg["source_tenant"],
                    internal_id=msg.get("internal_id"),
                    shipname=msg.get("shipname"),
                    callsign=msg.get("callsign"),
                    licence=msg.get("licence"),
                ),
            }
        )
