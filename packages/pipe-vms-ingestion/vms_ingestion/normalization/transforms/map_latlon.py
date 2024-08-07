import apache_beam as beam

LAT_LON_ALTERNATIVES = {
    "LATITUDE": "lat",
    "LONGITUDE": "lon",
}


def map_lat_lon(msg):
    result = {
        **msg,
    }
    for key in LAT_LON_ALTERNATIVES:
        if key in msg:
            lat_lon_field = LAT_LON_ALTERNATIVES[key]
            if lat_lon_field not in result:
                result[lat_lon_field] = result[key]

    return result


class MapLatLon(beam.PTransform):

    def expand(self, pcoll):
        return pcoll | beam.Map(map_lat_lon)
