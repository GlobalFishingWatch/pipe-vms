from apache_beam import Filter, PTransform


class DiscardZeroLatLon(PTransform):
    def expand(self, pcoll):
        return pcoll | self.discard_zero_lat_lon()

    def discard_zero_lat_lon(self):
        return Filter(
            lambda x: not (
                # exclude when lat, lon is  0, 0
                x.get("lat") == 0
                and x.get("lon") == 0
                #  exclude also if lat or lon is missing
                or x.get("lat") is None
                or x.get("lon") is None
            )
            # include only records that have lat and lon fields
            and ("lat" in x and "lon" in x)
        )
