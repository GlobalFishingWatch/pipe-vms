import unittest
from datetime import datetime

import apache_beam as beam
import pytest
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.ecu_map_source_message import (
    ECUMapSourceMessage,
    ecu_infer_shiptype,
)


class TestECUMapSourceMessage(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=ecu",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )
    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "utc_time": datetime.fromisoformat("2024-01-01 09:07:00+00:00"),
            "fechaqth": datetime.fromisoformat("2024-01-01 04:07:00+00:00"),
            "idnave": "98765",
            "idqth": "205703882",
            "lat": -2.5014833333,
            "lon": -79.82605,
            "matriculanave": "B -00-123456",
            "mmsi": None,
            "nombrenave": "SANTA MARIA I",
            "rumbo": "42",
            "velocidad": "21",
        },
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "B -00-123456",
            "course": 42.0,
            "internal_id": "98765",
            "lat": -2.5014833333,
            "lon": -79.82605,
            "shipname": "SANTA MARIA I",
            "shiptype": "boat",
            "speed": 21.0,
            "timestamp": datetime.fromisoformat("2024-01-01 09:07:00+00:00"),
        }
    ]

    # Tests the transform.
    def test_ecu_map_source_message(self):
        with TestPipeline(options=TestECUMapSourceMessage.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestECUMapSourceMessage.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | ECUMapSourceMessage()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestECUMapSourceMessage.EXPECTED),
                label="CheckOutput",
            )


@pytest.mark.parametrize(
    "matriculanave,shiptype",
    [
        ("B -00-11615", "boat"),
        ("DA-00-00545", "auxiliary"),
        (" B-07-04088 ", "boat"),
        ("TN -03-10534", "national traffic"),
        ("TI-00-00002", "international traffic"),
        ("P -00-00022", "fishing"),
        ("R -00-00107", "tug"),
        ("     ", "unknown"),
        (None, "unknown"),
        ("4040077351SP", "unknown"),
    ],
)
def test_ecu_infer_shiptype(matriculanave, shiptype):
    assert ecu_infer_shiptype(matriculanave) == shiptype
