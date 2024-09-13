from datetime import datetime

from apache_beam import Filter, PTransform


class FilterDateRange(PTransform):
    """Filter items whose timestamp field is inside the given date_rage.

    `date_range` (tuple[datetime, datetime]): A tuple composed by (start_date, end_date),
        the items will be filtered using the following condition:

        `start_date >= pcoll[timestamp] > end_date`

    input | FilterDateRange(date_range=(start_date, end_date))

    """

    def __init__(self, date_range: tuple[datetime, datetime], label=None) -> None:
        super().__init__(label)
        self.start_date, self.end_date = date_range
        self.with_input_types

    def expand(self, pcoll):
        return pcoll | Filter(
            lambda x: x["timestamp"] >= self.start_date
            and x["timestamp"] < self.end_date
        )
