def list_to_dict(labels):
    return {x.split("=")[0]: x.split("=")[1] for x in labels}


def to_float(value):
    return float(value) if value is not None else None


def to_string(value):
    return f"{value}".strip() if value is not None else None


def dms_to_float(dms_value: str):
    """
    Convert Degrees, Minutes, Seconds coordinate to float value

    :param dms_value: str Coordinate value sexpressed in Degrees, Minutes, Seconds
    :return: float Coordinate value converted to float
    """
    dms_value = dms_value.strip()
    degrees, rest = dms_value.split("°")
    minutes, rest = rest.split("'")
    seconds, direction = rest.split('" ')
    degrees = float(degrees)
    minutes = float(minutes)
    seconds = float(seconds)
    float_value = degrees + minutes / 60 + seconds / 3600
    if direction in ["S", "W"]:
        float_value = -float_value
    return float_value
