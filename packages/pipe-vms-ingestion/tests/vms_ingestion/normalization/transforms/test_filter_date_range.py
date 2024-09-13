import unittest
from datetime import datetime, timedelta

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.filter_date_range import FilterDateRange


class TestFilterDateRange(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=bra",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )

    # Tests the pipeline's transforms.
    def test_filter_date_range(self):
        with TestPipeline(options=TestFilterDateRange.options) as p:

            start_date = datetime.fromisoformat("2024-01-02 00:00:00+00:00")
            end_date = start_date + timedelta(days=1)

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(
                [
                    {"timestamp": start_date - timedelta(seconds=1)},
                    {"timestamp": start_date},
                    {"timestamp": end_date - timedelta(seconds=1)},
                    {"timestamp": end_date},
                ]
            )

            # Run the transform to test
            output: pvalue.PCollection = input | FilterDateRange(
                date_range=(start_date, end_date)
            )

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(
                    [
                        {"timestamp": start_date},
                        {"timestamp": end_date - timedelta(seconds=1)},
                    ]
                ),
                label="CheckOutput",
            )
