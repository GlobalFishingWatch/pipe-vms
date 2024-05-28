from datetime import datetime, timedelta


def prev_month_from_YYYYMMDD(dt_str):
    """
    This module returns the previous month in YYYYMMDD format
    of the current month received as a parameter.

    :param dt_str: String, date time in YYYYMMDD format of the current month
    :return: String, date time in YYYYMMDD format of the previous month
    """
    dt = datetime.strptime(dt_str, "%Y%m%d")
    dt = dt - timedelta(days=1)
    return datetime.strftime(dt.replace(day=1), "%Y%m%d")
