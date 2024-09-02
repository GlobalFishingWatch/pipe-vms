import functools
import re

import apache_beam as beam
from common.transforms.calculate_implied_course import calculate_implied_course
from common.transforms.calculate_implied_speed import calculate_implied_speed_kt
from utils.convert import to_float, to_string


def extract_shipname_from_name(name: str) -> str:
    if not name:
        return None
    separator = name.find(" - ")  # this was for appearisons of 'ex-' before separator.
    if separator > 0:
        return name[:separator]
    if re.match(r"^\([\w ]+\)[ \w ]+\(\w+\)$", name):
        return re.match(r"^\([\w ]+\)([ \w ]+)\(\w+\)$", name).group(1).strip()
    shipname_match = re.search(r"^[^\-\(]*", name)
    return shipname_match.group(0).strip() if shipname_match else None


def extract_short_shiptype_from_name(name: str) -> str:
    if not name:
        return None
    short_shiptype_match = re.search(r" - ([^\(]*)", name)
    return short_shiptype_match.group(1).upper() if short_shiptype_match else None


def extract_hsfl_from_name(name: str) -> str:
    if not name:
        return None
    if re.match(r"^\([\w ]+\)[ \w ]+\(\w+\)$", name):
        return re.match(r"^\(([\w ]+)\)[ \w ]+\(\w+\)$", name).group(1).strip()
    hsfl_match = re.search(r"([^\(]+)\)\(?[^\(]*\)$", name)
    return hsfl_match.group(1) if hsfl_match else None


def extract_callsign_from_name(name: str) -> str:
    if not name:
        return None
    callsign_match = re.search(r"\(([^\(]+)\)$", name)
    return callsign_match.group(1) if callsign_match else None


def extract_fields_from_name(msg):
    name = msg["name"]
    return {
        **msg,
        **dict(
            {
                "shipname": extract_shipname_from_name(name),
                "hsfl": extract_hsfl_from_name(name),
                "short_shiptype": extract_short_shiptype_from_name(name),
                "callsign": extract_callsign_from_name(name),
            }
        ),
    }


def set_previous_attr(group):
    k, msgs = group
    msgs_list = list(msgs)
    msgs_list.sort(key=lambda msg: msg["timestamp"])

    def set_attr(a, b):
        prev = a[-1] if len(a) > 0 else None
        b["prev_timestamp"] = prev["timestamp"] if prev else None
        b["prev_lat"] = prev["lat"] if prev else None
        b["prev_lon"] = prev["lon"] if prev else None
        a.append(b)
        return a

    return functools.reduce(set_attr, msgs_list, [])


def implies_speed_and_course(msg):
    res = msg.copy()
    speed = res.pop("speed")
    course = res.pop("course")
    if not speed:
        speed = calculate_implied_speed_kt(
            msg["prev_timestamp"],
            msg["prev_lat"],
            msg["prev_lon"],
            msg["timestamp"],
            msg["lat"],
            msg["lon"],
        )
    if not course:
        course = calculate_implied_course(
            msg["prev_lat"], msg["prev_lon"], msg["lat"], msg["lon"]
        )
    res["speed"] = speed
    res["course"] = course
    return res


shiptype_description = {
    "LL": "longline",
    "TRW": "trawler",
    "SV": "other_seines",
    "PS": "purse_seine",
}


def blz_map_source_message(msg):
    if "short_shiptype" in msg:
        if msg["short_shiptype"] in shiptype_description:
            msg["shiptype"] = shiptype_description[msg["short_shiptype"]]
        else:
            msg["shiptype"] = msg["short_shiptype"]

    return {
        "shipname": to_string(msg["shipname"]),
        "timestamp": msg["timestamp"],
        "received_at": msg["receiveDate"],
        "lat": to_float(msg["lat"]),
        "lon": to_float(msg["lon"]),
        "speed": to_float(msg["speed"]),
        "course": to_float(msg["course"]),
        "ssvid": to_string(msg["id"]),
        "callsign": to_string(msg["callsign"]),
        "shiptype": to_string(msg["shiptype"]),
        "imo": to_string(msg["imo"]),
    }


class BLZMapSourceMessage(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Extract From Name" >> beam.Map(extract_fields_from_name)
            | "Group by id" >> beam.GroupBy(id=lambda msg: msg["id"])
            | "Set previous timestamp,lat,lon" >> beam.FlatMap(set_previous_attr)
            | "Impling Speed and Course if not coming"
            >> beam.Map(implies_speed_and_course)
            | "Preliminary source fields mapping" >> beam.Map(blz_map_source_message)
        )
