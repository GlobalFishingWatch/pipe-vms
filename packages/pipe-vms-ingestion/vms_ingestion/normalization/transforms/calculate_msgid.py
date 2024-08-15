import hashlib


def get_raw_message_id(timestamp, lat, lon, ssvid, fleet, speed=None, course=None):
    iso_datetime = timestamp.strftime("%Y-%m-%d %H:%M:%S%z")
    # Adding semicolon to the timezone offset
    iso_datetime = f"{iso_datetime[0:-2]}:{iso_datetime[-2:]}"
    parts = [
        ssvid,
        *([f"{fleet}"] if fleet is not None else []),
        iso_datetime,
        f"{lat:.6f}",
        f"{lon:.6f}",
        *([f"{speed:.6f}"] if speed is not None else []),
        *([f"{course:.6f}"] if course is not None else []),
    ]
    return "|".join(parts)


def get_message_id(timestamp, lat, lon, ssvid, fleet, speed=None, course=None):
    raw_id = get_raw_message_id(timestamp, lat, lon, ssvid, fleet, speed, course)
    h = hashlib.blake2b(digest_size=32)
    h.update(str.encode(raw_id))
    return h.hexdigest()
