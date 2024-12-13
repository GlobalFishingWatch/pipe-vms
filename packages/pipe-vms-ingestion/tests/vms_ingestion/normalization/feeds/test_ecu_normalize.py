import os
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization.feeds.ecu_normalize import ECUNormalize

script_path = os.path.dirname(os.path.abspath(__file__))
FAKE_TIME = datetime(2024, 4, 20, 23, 59, 55)


class TestECUNormalize(unittest.TestCase):

    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            **x,
            "utc_time": datetime.fromisoformat(x["utc_time"]),
        }
        for x in read_json(f"{script_path}/data/raw_ecuador.json")
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "TN-00-01234",
            "class_b_cs_flag": None,
            "course": 117.0,
            "destination": None,
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "internal_id": "98765",
            "lat": -2.19445,
            "length": None,
            "lon": -80.0178,
            "msgid": "bef5614ce6fbdd7cf67b501ad4aba277d2b61208b37a3c8c8c36647108fcbc55",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "registry_number": "TN-00-01234",
            "shipname": "NAUTILUS XXX",
            "shiptype": "NATIONAL TRAFFIC",
            "source": "ECUADOR_VMS",
            "source_fleet": None,
            "source_provider": "DIRNEA",
            "source_ssvid": "98765",
            "source_tenant": "ECU",
            "source_type": "VMS",
            "speed": 5.0,
            "ssvid": "1768c607ea0b407a6fccad970fb9c57d14dced10753d3954d3f41964d8ea04e3",
            "status": None,
            "timestamp": datetime(2024, 4, 20, 8, 14, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 4, 20),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": "P -00-00123",
            "class_b_cs_flag": None,
            "course": 72.0,
            "destination": None,
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "internal_id": "12345",
            "lat": -7.611016,
            "length": None,
            "lon": -101.358451,
            "msgid": "2bbad4d39c55663e5618de268372a24c2b99bdf08c4bde7ef12f427ce7557b6e",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "registry_number": "P -00-00123",
            "shipname": "SANTA MARTA DOS",
            "shiptype": "FISHING",
            "source": "ECUADOR_VMS",
            "source_fleet": None,
            "source_provider": "DIRNEA",
            "source_ssvid": "12345",
            "source_tenant": "ECU",
            "source_type": "VMS",
            "speed": 13.0,
            "ssvid": "fa49046bb3d2c4d11465a62851f15e26b83713db82cb7da6d6ce6dab93d2cf74",
            "status": None,
            "timestamp": datetime(2024, 4, 20, 8, 0, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 4, 20),
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
        with TestPipeline() as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestECUNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | ECUNormalize(feed="ecu")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(output, pcol_equal_to(TestECUNormalize.EXPECTED), label="CheckOutput")
