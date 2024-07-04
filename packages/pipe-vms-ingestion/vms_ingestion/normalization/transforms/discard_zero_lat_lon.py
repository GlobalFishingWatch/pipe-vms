from apache_beam import Filter, PTransform


class DiscardZeroLatLon(PTransform):
    def expand(self, pcoll):
        return pcoll | self.discard_zero_lat_lon()

    def discard_zero_lat_lon(self):
        return Filter(lambda x: not (x["lat"] == 0 and x["lon"] == 0))
