import unittest
from datetime import datetime, timezone

import apache_beam as beam
import pytest
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.bra_map_source_message import (
    BRAMapSourceMessage,
    bra_infer_shiptype,
)


class TestBRAMapSourceMessage(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=bra",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )
    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "datahora": datetime.fromisoformat("2024-05-01 05:35:45+00:00"),
            "ID": "4961089",
            "mID": "181473822",
            "codMarinha": "210180889PA",
            "lat": "-1,21861112117767",
            "lon": "-48,4911117553711",
            "curso": "192",
            "nome": "Cibradep X",
            "speed": "17",
        },
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "",
            "course": 192.0,
            "internal_id": "4961089",
            "lat": -1.21861112117767,
            "lon": -48.4911117553711,
            "msgid": "181473822",
            "registry_number": "210180889PA",
            "shipname": "Cibradep X",
            "shiptype": "fishing",
            "speed_kph": 17.0,
            "timestamp": datetime(2024, 5, 1, 5, 35, 45, tzinfo=timezone.utc),
        }
    ]

    # Tests the transform.
    def test_bra_map_source_message(self):
        with TestPipeline(options=TestBRAMapSourceMessage.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestBRAMapSourceMessage.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | BRAMapSourceMessage()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestBRAMapSourceMessage.EXPECTED),
                label="CheckOutput",
            )


@pytest.mark.parametrize(
    "cod_marinha,shiptype",
    [
        ("UNDEFINED", ""),
        ("SERVICO", ""),
        ("Ç", ""),
        ("     ", ""),
        (None, ""),
        ("4040077351SP", "fishing"),
        ("4660011755RS", "fishing"),
    ],
)
def test_bra_infer_shiptype(cod_marinha, shiptype):
    assert bra_infer_shiptype(cod_marinha) == shiptype
