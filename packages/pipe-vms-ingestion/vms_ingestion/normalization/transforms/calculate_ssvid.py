import re


def encode_ssvid(country, internal_id="", shipname="", callsign="", licence="", **_):
    parts = [country]
    if internal_id:
        parts.append(f"i:{internal_id}")
    if shipname:
        parts.append(f"s:{shipname}")
    if callsign:
        parts.append(f"c:{callsign}")
    if licence:
        parts.append(f"l:{licence}")
    return "|".join(parts)


def decode_ssvid(ssvid):
    p = re.compile(
        "^(?P<country>[a-z]{3})"
        + "(?:\\|i:(?P<internal_id>.*?))?"
        + "(?:\\|s:(?P<shipname>.*?))?"
        + "(?:\\|c:(?P<callsign>.*?))?"
        + "(?:\\|l:(?P<licence>.*?))?$"
    )
    m = p.match(ssvid)
    return m.groupdict()
