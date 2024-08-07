from apache_beam import Filter, PTransform


class DiscardZeroLatLon(PTransform):
    def expand(self, pcoll):
        return pcoll | self.discard_zero_lat_lon()

    def discard_zero_lat_lon(self):
        return Filter(
            lambda x: not (
                (
                    "lat" in x
                    and "lon" in x
                    and x.get("lat") == 0
                    and x.get("lon") == 0
                    or x.get("lat") is None
                    or x.get("lon") is None
                )
                or (
                    "LATITUDE" in x
                    and "LONGITUDE" in x
                    and x.get("LATITUDE") == 0
                    and x.get("LONGITUDE") == 0
                    or x.get("LATITUDE") is None
                    or x.get("LONGITUDE") is None
                )
            )
        )
