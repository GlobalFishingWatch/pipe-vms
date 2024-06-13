from datetime import datetime

import apache_beam as beam
from vms_ingestion.normalization.transforms.calculate_msgid import \
    get_message_id
from vms_ingestion.normalization.transforms.calculate_ssvid import encode_ssvid


def map_normalized_message(msg, feed, source_provider, source_format):
    result = {
        "source_type": "VMS",
        "source_tenant": feed.upper(),
        "source_provider": source_provider,
        "source_fleet": msg.get("fleet"),
        "type": msg.get("type", "VMS"),
        "timestamp": msg["timestamp"],
        "lat": msg["lat"],
        "lon": msg["lon"],
        "speed": msg.get("speed"),
        "course": msg.get("course"),
        "heading": msg.get("heading"),
        "shipname": msg["shipname"],
        "callsign": msg["callsign"] if msg["callsign"] else None,
        "destination": msg.get("destination"),
        "imo": msg.get("imo"),
        "shiptype": msg.get("shiptype"),
        "receiver_type": msg.get("receiver_type"),
        "receiver": msg.get("receiver"),
        "length": msg.get("length"),
        "width": msg.get("width"),
        "status": msg.get("status"),
        "class_b_cs_flag": msg.get('class_b_cs_flag'),
        "received_at": msg.get('received_at'),
        "ingested_at": msg.get('ingested_at'),
        "timestamp_date": datetime.date(msg["timestamp"]),
    }
    return {**result, "source": source_format.format(**result)}


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
        return beam.Map(lambda msg: map_normalized_message(msg=msg,
                                                           feed=self.feed,
                                                           source_provider=self.source_provider,
                                                           source_format=self.source_format,
                                                           ))

    def calculate_message_id(self):
        return beam.Map(lambda msg: dict(**msg, msgid=get_message_id(timestamp=msg["timestamp"],
                                                                     lat=msg["lat"],
                                                                     lon=msg["lon"],
                                                                     ssvid=msg["ssvid"],
                                                                     fleet=msg.get("fleet"),
                                                                     speed=msg.get("speed"),
                                                                     course=msg.get("course"),
                                                                     )))

    def calculate_ssvid(self):
        return beam.Map(lambda msg: dict(**msg, ssvid=encode_ssvid(country=msg["source_tenant"],
                                                                   internal_id=msg.get("internal_id"),
                                                                   shipname=msg.get("shipname"),
                                                                   callsign=msg.get("callsign"),
                                                                   licence=msg.get("licence"))))
