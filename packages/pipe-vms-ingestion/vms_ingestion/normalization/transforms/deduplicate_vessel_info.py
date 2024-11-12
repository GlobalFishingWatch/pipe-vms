from apache_beam import GroupBy, Map, PTransform


def choose_from_group(group):
    _, msgs = group
    msgs_list = list(msgs)
    sorted_messages = [*msgs_list]
    # Sort the messages, most recent at the top
    sorted_messages.sort(
        key=lambda x: (
            x["timestamp"].timestamp()
            if x["timestamp"]
            else x["received_at"].timestamp() if x["received_at"] else 0
        ),
        reverse=True,
    )
    proposed_msg = msgs_list[0]
    if len(sorted_messages) > 0:
        proposed_msg = sorted_messages[0]

    return proposed_msg


class DeduplicateVesselInfo(PTransform):
    """The id of the record is the combination of ssvid.
    First we tag with this id and then group by it.
    Later we prioritize most recent messages."""

    def expand(self, pcoll):
        return pcoll | self.group_by_ssvid_timestamp() | self.prioritize_most_recent()

    def group_by_ssvid_timestamp(self):
        return GroupBy(
            id=lambda message: message["ssvid"],
        )

    def prioritize_most_recent(self):
        return Map(choose_from_group)
