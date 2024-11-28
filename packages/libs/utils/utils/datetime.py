from datetime import datetime, timezone


def parse_yyyy_mm_dd_param(value, tzinfo=timezone.utc):
    """
    Parse a date string in the format "yyyy-mm-dd" and return a `datetime` object with the specified tzinfo.

    Args:
        value (str): The date string to parse.
        tzinfo (timezone, optional): The time zone info to use for the resulting datetime object.
        Defaults to timezone.utc.

    Returns:
        `datetime`: A `datetime` object representing the parsed date and time.
    """
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=tzinfo)


def now(tz=timezone.utc):
    """
    Return the current date and time in the specified timezone.

    Args:
        tz (timezone, optional): The time zone to use for the resulting datetime object. Defaults to timezone.utc.

    Returns:
        `datetime`: A `datetime` object representing the current date and time in the specified timezone.
    """
    return datetime.now(tz=tz)
