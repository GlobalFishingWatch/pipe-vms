

from hashlib import md5


def get_raw_message_id(source, timestamp, lat, lon, shipname, callsign):
    iso_datetime = timestamp.strftime("%Y-%m-%d %H:%M:%S%z")
    # Adding semicolon to the timezone offset
    iso_datetime = f'{iso_datetime[0:-2]}:{iso_datetime[-2:]}'
    parts = [source,
             iso_datetime,
             f'{lat:.6f}',
             f'{lon:.6f}',
             f'{shipname}'.strip(),
             f'{callsign}'.strip(),
             ]
    return '|'.join(parts)


def get_md5_message_id(source, timestamp, lat, lon, shipname, callsign):
    return md5(get_raw_message_id(source, timestamp, lat, lon, shipname, callsign).encode('utf-8'))


def get_message_id(source, timestamp, lat, lon, shipname, callsign):
    return get_md5_message_id(source, timestamp, lat, lon, shipname, callsign).hexdigest()
