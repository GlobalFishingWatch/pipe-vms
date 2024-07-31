import unittest
from datetime import datetime

import apache_beam as beam
import pytest
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.nor_map_source_message import (
    NORMapSourceMessage,
    nor_infer_shiptype,
)


class TestNORMapSourceMessage(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=nor",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )
    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "message_id": "229526703",
            "message_type_code": "POS",
            "message_type": "Posisjonsrapport",
            "timestamp_utc": datetime.fromisoformat("2024-07-02 20:18:00+00:00"),
            "lat": "68.6139",
            "lon": "14.253",
            "course": "150",
            "speed": "1.9",
            "callsign": "LLQX",
            "registration_mark": "Ø 0300H",
            "vessel_name": "    SPJÆRINGEN     ",
            "length": "27.91",
            "gross_tonnage": "272",
            "power_engine": "900",
            "vessel_type": "Fiskefartøy",
        },
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "shipname": "SPJÆRINGEN",
            "msgid": "229526703",
            "timestamp": datetime.fromisoformat("2024-07-02 20:18:00+00:00"),
            "lat": 68.6139,
            "length": 27.91,
            "lon": 14.253,
            "speed": 1.9,
            "course": 150.0,
            "shiptype": "fishing",
            "callsign": "LLQX",
        }
    ]

    # Tests the transform.
    def test_nor_map_source_message(self):
        with TestPipeline(options=TestNORMapSourceMessage.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestNORMapSourceMessage.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | NORMapSourceMessage()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestNORMapSourceMessage.EXPECTED),
                label="CheckOutput",
            )


@pytest.mark.parametrize(
    "vessel_type,shiptype",
    [
        ("    fiskefartøy    ", "fishing"),
        ("FISKEFARTØY", "fishing"),
        ("FISKEFARTØY (AKTIV)", "fishing"),
        ("   fiskefartøy (aktiv)    ", "fishing"),
        ("   FORSKNINGSSKIP  ", "research"),
        ("   forskningsskip  ", "research"),
        ("   TARETRÅLER  ", "kelp trawlers"),
        ("   taretråler  ", "kelp trawlers"),
        ("   OTHER   ", "other"),
    ],
)
def test_nor_infer_shiptype(vessel_type, shiptype):
    assert nor_infer_shiptype(vessel_type) == shiptype
