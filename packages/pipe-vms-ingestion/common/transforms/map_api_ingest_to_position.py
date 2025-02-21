from datetime import datetime, timezone

import apache_beam as beam
from utils.datetime import now


def get_datetime_with_millis(datetime_value):
    """
    Ensure the datetime contains milliseconds.
    """
    # ensure the datetime contains milliseconds
    suffix = f".000{datetime_value[19:]}"[-5:]
    return f"{datetime_value[:19]}{suffix}"


def map_api_ingest_to_position(msg):
    position_msg = msg.get("message")
    # flatten the message
    position_msg = {**position_msg.get("extraInfo", {}), **position_msg}
    common_msg = msg.get("common")
    result = {
        "position": {
            "callsign": position_msg.get("callsign"),
            "course": position_msg.get("course"),
            "external_id": position_msg.get("external_id"),
            "flag": position_msg.get("flag"),
            "imo": position_msg.get("imo"),
            "ingested_at": now(tz=timezone.utc),
            "internal_id": position_msg.get("id"),
            "lat": position_msg.get("lat"),
            "lon": position_msg.get("lon"),
            "mmsi": position_msg.get("mmsi"),
            "received_at": datetime.strptime(
                get_datetime_with_millis(position_msg["receiveDate"]), "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=timezone.utc),
            "shipname": position_msg.get("name"),
            "source_fleet": common_msg.get("fleet"),
            "source_provider": common_msg.get("provider"),
            "source_tenant": common_msg.get("country"),
            "source_type": f"VMS-{common_msg.get('format')}",
            "speed": position_msg.get("speed"),
            "timestamp": datetime.strptime(
                get_datetime_with_millis(position_msg["timestamp"]), "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=timezone.utc),
            "extra_fields": {},
        },
        "common": common_msg,
    }
    return result


class MapAPIIngestToPosition(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | beam.Map(map_api_ingest_to_position)
