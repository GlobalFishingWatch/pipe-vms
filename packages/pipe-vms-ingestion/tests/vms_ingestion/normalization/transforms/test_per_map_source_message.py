import unittest
from datetime import datetime

import apache_beam as beam
import pytest
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.per_map_source_message import (
    PERMapSourceMessage,
    per_infer_fleet,
)


class TestPERMapSourceMessage(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=per",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )
    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "Correlativo": "1",
            "ID": "2.4073100000071803e+18",
            "DATETRANSMISSION": datetime.fromisoformat("2024-07-31 00:00:00+00:00"),
            "COASTLENGHT": 44.86393,
            "LATITUDE": -11.71153,
            "LONGITUDE": -78.28806,
            "SPEED": 0.5,
            "COURSE": 245.0,
            "EVENTDESCRIPTION": "NO DEFINIDO",
            "PLATE": "TA-12345-OP",
            "nickname": "SANTA MARIA",
            "SOURCE": "MEGATRACK",
            "ESPECIESCHI": "",
            "ESPECIESCHD": "PER/POT",
            "DESC_REGIMEN": "DS. 006-2016-PRODUCE",
        }
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": None,
            "course": 245.0,
            "fleet": "artisanal",
            "lat": -11.71153,
            "lon": -78.28806,
            "shipname": "SANTA MARIA",
            "speed": 0.5,
            "ssvid": "TA-12345-OP",
            "timestamp": datetime.fromisoformat("2024-07-31 05:00:00+00:00"),
        }
    ]

    # Tests the transform.
    def test_per_map_source_message(self):
        with TestPipeline(options=TestPERMapSourceMessage.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPERMapSourceMessage.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | PERMapSourceMessage()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestPERMapSourceMessage.EXPECTED),
                label="CheckOutput",
            )


@pytest.mark.parametrize(
    "desc_regimen,fleet",
    [
        ("  DECRETO LEGISLATIVO N° 1392   ", "artisanal"),
        (
            "  DECRETO LEGISLATIVO N 1392   ",
            "artisanal",
        ),
        ("  DECRETO LEGISLATIVO Nº1273   ", "artisanal"),
        (
            "  DECRETO LEGISLATIVO N° 1273   ",
            "artisanal",
        ),
        (
            "  DECRETO LEGISLATIVO  N 1273   ",
            "artisanal",
        ),
        (
            "  DS. 006-2016-PRODUCE   ",
            "artisanal",
        ),
        (
            "  NO ESPECIFICADO   ",
            "artisanal",
        ),
        (
            "  MENOR ESCALA   ",
            "small-scale",
        ),
        (
            "  MENOR ESCALA (ANCHOVETA) - ARTESANAL   ",
            "small-scale",
        ),
        (
            "  DS. 020-2011-PRODUCE   ",
            "small-scale",
        ),
        (
            "  MENOR ESCALA (ANCHOVETA)   ",
            "small-scale",
        ),
        (
            "  DECRETO LEY Nº25977   ",
            "industrial",
        ),
        (
            "  DS. 022-2009-PRODUCE   ",
            "industrial",
        ),
        (
            "  D.S 022-2009-PRODUCE   ",
            "industrial",
        ),
        ("  DS. 032-2003-PRODUCE   ", "industrial"),
        (
            "  DS. 016-2020-PRODUCE   ",
            "industrial",
        ),
        (
            "  LEY Nº26920   ",
            "industrial",
        ),
        (
            "some invalid regimen ",
            "not defined",
        ),
    ],
)
def test_per_infer_fleet(desc_regimen, fleet):
    assert per_infer_fleet(desc_regimen) == fleet
