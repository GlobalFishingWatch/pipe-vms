import unittest
from datetime import datetime

import apache_beam as beam
import pytest
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization.transforms.per_map_source_message import PERMapSourceMessage, per_infer_fleet


class TestPERMapSourceMessage(unittest.TestCase):

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
        },
        {
            "Correlativo": "41",
            "ID": "2.4073014212795151e+18",
            "DATETRANSMISSION": datetime.fromisoformat("2024-07-30 14:21:38"),
            "COASTLENGHT": "53.85904",
            "LATITUDE": "-10.15091",
            "LONGITUDE": "-79.12178",
            "SPEED": "4.3",
            "COURSE": "271.0",
            "EVENTDESCRIPTION": "NO DEFINIDO",
            "PLATE": "412412412",
            "nickname": "NAUTILUS CHN II",
            "SOURCE": "CLS",
            "ESPECIESCHI": "",
            "ESPECIESCHD": "ANGL/SAR/R.H./ANCH/B",
            "DESC_REGIMEN": "DECRETO LEGISLATIVO N 1392",
        },
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": None,
            "course": 245.0,
            "flag": None,
            "fleet": "artisanal",
            "lat": -11.71153,
            "lon": -78.28806,
            "mmsi": None,
            "registry_number": "TA-12345-OP",
            "shipname": "SANTA MARIA",
            "speed": 0.5,
            "ssvid": "TA-12345-OP",
            "timestamp": datetime.fromisoformat("2024-07-31 05:00:00+00:00"),
        },
        {
            "callsign": None,
            "course": 271.0,
            "flag": "CHN",
            "fleet": "artisanal",
            "lat": -10.15091,
            "lon": -79.12178,
            "mmsi": "412412412",
            "registry_number": "412412412",
            "shipname": "NAUTILUS CHN II",
            "speed": 4.3,
            "ssvid": "412412412",
            "timestamp": datetime(2024, 7, 30, 19, 21, 38),
        },
    ]

    # Tests the transform.
    def test_per_map_source_message(self):
        with TestPipeline() as p:

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
