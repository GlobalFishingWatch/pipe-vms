import math


def calculate_implied_course(lat1, lon1, lat2, lon2):
    if None in [lat1, lon1, lat2, lon2]:
        return None
    # Calculates forward azimuth - course along a great circle
    # from a to b
    alat = math.radians(lat1)
    alon = math.radians(lon1)
    blat = math.radians(lat2)
    blon = math.radians(lon2)
    dlon = blon - alon
    return (
        360
        + math.degrees(
            math.atan2(
                math.sin(dlon) * math.cos(blat),
                math.cos(alat) * math.sin(blat)
                - math.sin(alat) * math.cos(blat) * math.cos(dlon),
            )
        )
    ) % 360
