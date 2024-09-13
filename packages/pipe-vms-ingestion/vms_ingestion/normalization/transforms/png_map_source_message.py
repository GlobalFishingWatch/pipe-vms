import functools

import apache_beam as beam
import pycountry
from common.transforms.calculate_implied_course import calculate_implied_course
from common.transforms.calculate_implied_speed import calculate_implied_speed_kt
from shipdataprocess.normalize import normalize_callsign
from utils.convert import to_float, to_string


def prioritize_msg_with_name(group):
    key, msgs = group
    msgs_list = list(msgs)
    # Giving priority to msgs that comes with name than others.
    msgs_with_name = [msg for msg in msgs_list if msg["shipname"] is not None]

    proposed_msg = msgs_list[0]
    if len(msgs_with_name) > 0:
        proposed_msg = msgs_with_name[0]

    return proposed_msg


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


def get_country_alpha_3(flag):
    if len(flag) == 2:
        return pycountry.countries.get(alpha_2=flag).alpha_3
    return flag


def ensure_internal_id(msg):
    return {
        **msg,
        "internal_id": (
            msg["internal_id"] or normalize_callsign(to_string(msg["callsign"]))
        ),
    }


def png_map_source_message(msg):
    return {
        "callsign": to_string(msg["callsign"]),
        "course": to_float(msg["course"]),
        "flag": get_country_alpha_3(msg.get("flag")),
        "lat": to_float(msg["lat"]),
        "lon": to_float(msg["lon"]),
        "shipname": to_string(msg["shipname"]),
        "speed": to_float(msg["speed"]),
        "timestamp": msg["timestamp"],
    }


class PNGMapSourceMessage(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Ensure internal id" >> beam.Map(ensure_internal_id)
            | "Group by id" >> beam.GroupBy(id=lambda msg: msg["internal_id"])
            | "Set previous timestamp,lat,lon" >> beam.FlatMap(set_previous_attr)
            | "Impling Speed and Course if not coming"
            >> beam.Map(implies_speed_and_course)
            | "Preliminary source fields mapping" >> beam.Map(png_map_source_message)
        )
