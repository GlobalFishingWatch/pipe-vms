from datetime import datetime, timezone

import apache_beam as beam
from shipdataprocess.standardize import standardize_str
from utils.datetime import now


def get_datetime_value(naf_msg):
    """
    Get the datetime value from the message replacing the spaces with 0 and ensuring the date and time are in the
    correct format to be parsed.
    """
    # ensure the date is in the format YYYYMMDD
    date = f"20{naf_msg['DA']}"[-8:]
    # ensure the time is in the format HHMMSS
    time = f"{naf_msg['TI']}00"[:6]
    # return the datetime value in the format YYYYMMDDHHMMSS
    return f"{date}{time}".replace(" ", "0")


def map_naf_to_position(msg):
    naf_msg = msg.get("message")
    common_msg = msg.get("common")
    result = {
        "position": {
            "callsign": naf_msg.get("RC"),
            "course": naf_msg.get("CO"),
            "external_id": naf_msg.get("XR"),
            "flag": naf_msg.get("FS"),
            "imo": naf_msg.get("XR"),
            "ingested_at": now(tz=timezone.utc),
            "internal_id": naf_msg.get("IR"),
            "lat": naf_msg.get("LT"),
            "lon": naf_msg.get("LG"),
            "mmsi": None,
            "received_at": common_msg.get("received_at"),
            "shipname": naf_msg.get("NA"),
            "source_fleet": standardize_str(common_msg.get("fleet")),
            "source_provider": standardize_str(common_msg.get("provider")),
            "source_tenant": standardize_str(common_msg.get("country")),
            "source_type": f"VMS-{common_msg.get('format')}",
            "speed": naf_msg.get("SP"),
            "timestamp": datetime.strptime(get_datetime_value(naf_msg), "%Y%m%d%H%M%S"),
        },
        "common": common_msg,
    }
    return result


class MapNAFToPosition(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | beam.Map(map_naf_to_position)
