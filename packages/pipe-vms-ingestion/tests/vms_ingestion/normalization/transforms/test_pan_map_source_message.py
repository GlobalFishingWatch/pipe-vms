import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.pan_map_source_message import (
    PANMapSourceMessage,
)


class TestPANMapSourceMessage(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=pan",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )
    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "callsign": "CM-98723",
            "course": 245.0,
            "imo": "4486393",
            "lat": -11.71153,
            "lon": -78.28806,
            "mmsi": "967546990",
            "shipname": "SANTA MARIA I",
            "shiptype": "tanker",
            "speed": 0.5,
            "timestamp": datetime.fromisoformat("2024-07-31 00:00:00+00:00"),
        }
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "CM-98723",
            "course": 245.0,
            "imo": "4486393",
            "lat": -11.71153,
            "lon": -78.28806,
            "mmsi": "967546990",
            "shipname": "SANTA MARIA I",
            "shiptype": "tanker",
            "speed": 0.5,
            "ssvid": "SANTAMARIA1",
            "timestamp": datetime.fromisoformat("2024-07-31 00:00:00+00:00"),
        }
    ]

    # Tests the transform.
    def test_pan_map_source_message(self):
        with TestPipeline(options=TestPANMapSourceMessage.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPANMapSourceMessage.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | PANMapSourceMessage()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestPANMapSourceMessage.EXPECTED),
                label="CheckOutput",
            )


# @pytest.mark.parametrize(
#     "desc_regimen,fleet",
#     [
#         ("  DECRETO LEGISLATIVO N° 1392   ", "artisanal"),
#         (
#             "  DECRETO LEGISLATIVO N 1392   ",
#             "artisanal",
#         ),
#         ("  DECRETO LEGISLATIVO Nº1273   ", "artisanal"),
#         (
#             "  DECRETO LEGISLATIVO N° 1273   ",
#             "artisanal",
#         ),
#         (
#             "  DECRETO LEGISLATIVO  N 1273   ",
#             "artisanal",
#         ),
#         (
#             "  DS. 006-2016-PRODUCE   ",
#             "artisanal",
#         ),
#         (
#             "  NO ESPECIFICADO   ",
#             "artisanal",
#         ),
#         (
#             "  MENOR ESCALA   ",
#             "small-scale",
#         ),
#         (
#             "  MENOR ESCALA (ANCHOVETA) - ARTESANAL   ",
#             "small-scale",
#         ),
#         (
#             "  DS. 020-2011-PRODUCE   ",
#             "small-scale",
#         ),
#         (
#             "  MENOR ESCALA (ANCHOVETA)   ",
#             "small-scale",
#         ),
#         (
#             "  DECRETO LEY Nº25977   ",
#             "industrial",
#         ),
#         (
#             "  DS. 022-2009-PRODUCE   ",
#             "industrial",
#         ),
#         (
#             "  D.S 022-2009-PRODUCE   ",
#             "industrial",
#         ),
#         ("  DS. 032-2003-PRODUCE   ", "industrial"),
#         (
#             "  DS. 016-2020-PRODUCE   ",
#             "industrial",
#         ),
#         (
#             "  LEY Nº26920   ",
#             "industrial",
#         ),
#         (
#             "some invalid regimen ",
#             "not defined",
#         ),
#     ],
# )
# def test_pan_infer_fleet(desc_regimen, fleet):
#     assert pan_infer_fleet(desc_regimen) == fleet
