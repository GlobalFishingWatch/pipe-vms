import pytest
from common.transforms.calculate_implied_speed import calculate_implied_speed_kt
from tests.vms_ingestion.common.transforms.data.implied_speed_course import (
    MESSAGES_WITH_IMPLIED_SPEED_AND_COURSE,
)


class TestCalculateImpliedSpeed:

    @pytest.mark.parametrize(
        "id,timestamp,lat,lon,course,speed,prev_timestamp,prev_lat,prev_lon",
        MESSAGES_WITH_IMPLIED_SPEED_AND_COURSE,
    )
    def test_calculate_implied_speed_kt(
        self, id, timestamp, lat, lon, course, speed, prev_timestamp, prev_lat, prev_lon
    ):
        implied_speed = calculate_implied_speed_kt(
            prev_timestamp, prev_lat, prev_lon, timestamp, lat, lon
        )

        speed_is_not_defined = (
            implied_speed is None
            and prev_timestamp is None
            and prev_lat is None
            and prev_lon is None
        )
        assert speed_is_not_defined or implied_speed == pytest.approx(speed, abs=0.1)
