from apache_beam import GroupBy, Map, PTransform


def choose_from_group(group):
    _, msgs = group
    msgs_list = list(msgs)
    # Giving priority to msgs that comes with shipname than others.
    msgs_with_name = [msg for msg in msgs_list if msg["shipname"] is not None]
    # Sort the messages, most recent at the top
    msgs_with_name.sort(
        key=lambda x: x["received_at"].timestamp() if x["received_at"] else 0,
        reverse=True,
    )
    proposed_msg = msgs_list[0]
    if len(msgs_with_name) > 0:
        proposed_msg = msgs_with_name[0]

    return proposed_msg


class DeduplicateMsgs(PTransform):
    """The id of the record is the combination of ssvid and timestamp.
    First we tag with this id and then group by it.
    Later we prioritize the messages that has content in shipname field."""

    def expand(self, pcoll):
        return (
            pcoll
            | self.group_by_ssvid_timestamp()
            | self.prioritize_msg_with_shipname()
        )

    def group_by_ssvid_timestamp(self):
        return GroupBy(
            id=lambda message: message["ssvid"],
            timestamp=lambda message: message["timestamp"].isoformat(),
        )

    def prioritize_msg_with_shipname(self):
        return Map(choose_from_group)
