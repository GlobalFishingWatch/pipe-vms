import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.chl_normalize import CHLNormalize


class TestCHLNormalize(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=chl",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )

    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "shipname": "AUSTRAL TRAVELER",
            "timestamp": datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
            "lat": -52.546,
            "lon": -71.947,
            "speed": 9.0,
            "course": 37.0,
            "callsign": "ABC123",
            "fleet": "some_fleet",
        },
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "msgid": "c8de2f410ae274c94fbd6583f722fc6b",
            "source": "CHILE_VMS_SOME_FLEET",
            "source_type": "VMS",
            "source_tenant": "CHL",
            "source_provider": "SERNAPESCA",
            "source_fleet": "SOME_FLEET",
            "source_ssvid": None,
            "type": "VMS",
            "ssvid": "CHL|s:AUSTRALTRAVELER|c:ABC123",
            "timestamp": datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
            "lat": -52.546,
            "lon": -71.947,
            "speed": 9.0,
            "course": 37.0,
            "heading": None,
            "shipname": "AUSTRAL TRAVELER",
            "callsign": "ABC123",
            "fleet": "some_fleet",
            "destination": None,
            "imo": None,
            "shiptype": None,
            "receiver_type": None,
            "receiver": None,
            "length": None,
            "width": None,
            "status": None,
            "class_b_cs_flag": None,
            "received_at": None,
            "ingested_at": None,
            "timestamp_date": datetime.date(
                datetime.fromisoformat("2020-01-01 20:23:01+00:00")
            ),
        }
    ]

    # Example test that tests the pipeline's transforms.
    def test_normalize(self):
        with TestPipeline(options=TestCHLNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestCHLNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | CHLNormalize(feed="chl")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestCHLNormalize.EXPECTED), label="CheckOutput"
            )
