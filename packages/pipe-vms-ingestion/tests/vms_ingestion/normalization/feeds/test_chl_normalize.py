import unittest
from datetime import datetime
from unittest.mock import patch

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.chl_normalize import CHLNormalize

FAKE_TIME = datetime(2020, 2, 3, 17, 5, 55)


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
            "msgid": "053c38484d2359001ff83e49def82f462d287a36ad2019b9bf552eca8395486e",
            "source": "CHILE_VMS_SOME_FLEET",
            "source_type": "VMS",
            "source_tenant": "CHL",
            "source_provider": "SERNAPESCA",
            "source_fleet": "SOME_FLEET",
            "source_ssvid": None,
            "type": "VMS",
            "ssvid": "7c430258c561efded2e98941306e20bb890204dfebb092f57273928472ba2029",
            "timestamp": datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
            "lat": -52.546,
            "lon": -71.947,
            "speed": 9.0,
            "course": 37.0,
            "heading": None,
            "flag": None,
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
            "updated_at": FAKE_TIME,
            "timestamp_date": datetime.date(
                datetime.fromisoformat("2020-01-01 20:23:01+00:00")
            ),
        }
    ]

    # Example test that tests the pipeline's transforms.
    @patch(
        "vms_ingestion.normalization.transforms.map_normalized_message.now",
        side_effect=lambda tz: FAKE_TIME,
    )
    def test_normalize(self, mock_now):
        with TestPipeline(options=TestCHLNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestCHLNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | CHLNormalize(feed="chl")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestCHLNormalize.EXPECTED), label="CheckOutput"
            )
