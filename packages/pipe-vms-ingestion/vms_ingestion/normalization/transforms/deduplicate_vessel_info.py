from apache_beam import GroupBy, Map, PTransform


def choose_from_group(group):
    def sort_key(x):
        return x["timestamp"].timestamp() if x["timestamp"] else x["received_at"].timestamp() if x["received_at"] else 0

    _, msgs = group
    sorted_msgs = sorted(msgs, key=sort_key, reverse=True)
    return sorted_msgs[0]


class DeduplicateVesselInfo(PTransform):
    """The id of the record is the combination of ssvid and year,
    since there should be at least one vessel info record per year.
    First we tag with this id and then group by it.
    Later we prioritize most recent messages."""

    def expand(self, pcoll):
        return pcoll | self.group_by_ssvid_year() | self.prioritize_most_recent()

    def group_by_ssvid_year(self):
        return GroupBy(
            id=lambda message: message["ssvid"],
            year=lambda message: message["timestamp"].year,
        )

    def prioritize_most_recent(self):
        return Map(choose_from_group)
