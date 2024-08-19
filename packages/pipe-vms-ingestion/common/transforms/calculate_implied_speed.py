from haversine import Unit, haversine


def calculate_implied_speed_kt(ts1, lat1, lon1, ts2, lat2, lon2):
    if None in [ts1, lat1, lon1, ts2, lat2, lon2]:
        return None
    prev = (lat1, lon1)
    current = (lat2, lon2)
    distance_kt = abs(haversine(current, prev, unit=Unit.NAUTICAL_MILES))
    timedelta_h = abs((ts2 - ts1).total_seconds() / (60 * 60))
    return 0 if timedelta_h == 0 else (distance_kt / timedelta_h)
