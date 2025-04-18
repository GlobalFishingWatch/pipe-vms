import unittest
from datetime import datetime
from unittest.mock import patch

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.cri_normalize import CRINormalize

FAKE_TIME = datetime(2020, 2, 3, 17, 5, 55)


class TestCRINormalize(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=cri",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )

    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "timestamp": datetime.fromisoformat("2024-05-01 12:15:01+00:00"),
            "callsign": None,
            "shipname": "K\u0027IN",
            "internal_id": None,
            "external_id": "P-10371",
            "registry_number": None,
            "lat": 9.9798,
            "lon": -84.8221,
            "speed": 0.0,
            "course": 0.0,
            "flag": None,
            "fleet": "sardineros",
        }
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 0.0,
            "destination": None,
            "external_id": "P-10371",
            "flag": None,
            "fleet": "sardineros",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "internal_id": None,
            "lat": 9.9798,
            "length": None,
            "lon": -84.8221,
            "msgid": "91d8aa18a38f18694baf367db93ecd067365a44e069f896a209889b097a5d74c",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "registry_number": None,
            "shipname": "K'IN",
            "shiptype": None,
            "source": "COSTARICA_VMS_SARDINEROS",
            "source_fleet": "SARDINEROS",
            "source_provider": "INCOPESCA",
            "source_tenant": "CRI",
            "source_ssvid": None,
            "source_type": "VMS",
            "speed": 0.0,
            "ssvid": "e41ebf65d65dd6355e6665e272320a8a552c2e68d6374a4dec601ed2aeb54b6b",
            "status": None,
            "timestamp": datetime.fromisoformat("2024-05-01 12:15:01+00:00"),
            "timestamp_date": datetime.date(
                datetime.fromisoformat("2024-05-01 12:15:01+00:00")
            ),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
    ]

    # Example test that tests the pipeline's transforms.
    @patch(
        "vms_ingestion.normalization.transforms.map_normalized_message.now",
        side_effect=lambda tz: FAKE_TIME,
    )
    def test_normalize(self, mock_now):
        with TestPipeline(options=TestCRINormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestCRINormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | CRINormalize(feed="cri")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestCRINormalize.EXPECTED), label="CheckOutput"
            )
