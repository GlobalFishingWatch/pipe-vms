import pytest
from common.transforms.calculate_implied_course import calculate_implied_course
from tests.vms_ingestion.common.transforms.data.implied_speed_course import (
    MESSAGES_WITH_IMPLIED_SPEED_AND_COURSE,
)


class TestCalculateImpliedCourse:

    @pytest.mark.parametrize(
        "id,timestamp,lat,lon,course,speed,prev_timestamp,prev_lat,prev_lon",
        MESSAGES_WITH_IMPLIED_SPEED_AND_COURSE,
    )
    def test_calculate_implied_course(
        self, id, timestamp, lat, lon, course, speed, prev_timestamp, prev_lat, prev_lon
    ):
        implied_course = calculate_implied_course(prev_lat, prev_lon, lat, lon)

        course_is_not_defined = (
            implied_course is None
            and prev_timestamp is None
            and prev_lat is None
            and prev_lon is None
        )
        # when course is defined
        # calculated course matches approximatelly (1% tolerance) the expected course
        # near the 360º calculated course matches approximatelly (1% tolerance) the expected course
        assert (
            course_is_not_defined
            or implied_course == pytest.approx(course, abs=360 * 0.01)
            or abs(360 - implied_course) == pytest.approx(course, abs=360 * 0.01)
        )
