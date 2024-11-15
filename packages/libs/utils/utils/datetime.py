from datetime import datetime, timedelta, timezone


def prev_month_from_YYYYMMDD(dt_str):
    """
    Returns the previous month from a given date in YYYYMMDD format.

    Args:
        dt_str (str): The date in YYYYMMDD format.

    Returns:
        str: The previous month from the given date.
    """
    dt = datetime.strptime(dt_str, "%Y%m%d")
    dt = dt - timedelta(days=1)
    return datetime.strftime(dt.replace(day=1), "%Y%m%d")


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
