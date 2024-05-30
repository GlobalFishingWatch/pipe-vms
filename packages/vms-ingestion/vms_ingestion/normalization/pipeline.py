import datetime as dt

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from vms_ingestion.normalization.feed_normalization_pipeline import \
    FeedNormalizationPipeline
from vms_ingestion.normalization.feed_pipeline_factory import \
    FeedPipelineFactory
from vms_ingestion.normalization.options import NormalizationOptions


def parse_yyyy_mm_dd_param(value):
    return dt.datetime.strptime(value, "%Y-%m-%d")


def list_to_dict(labels): return {x.split(
    '=')[0]: x.split('=')[1] for x in labels}


class NormalizationPipeline:
    def __init__(self, options):
        self.pipeline = beam.Pipeline(options=options)

        params = options.view_as(NormalizationOptions)
        gCloudParams = options.view_as(GoogleCloudOptions)

        start_date = parse_yyyy_mm_dd_param(params.start_date)
        end_date = parse_yyyy_mm_dd_param(params.end_date)
        labels = list_to_dict(gCloudParams.labels)

        # Retrieve the feed pipeline for the given country
        constructor = FeedPipelineFactory.get_pipeline(feed=params.country_code)

        feed_pipeline: FeedNormalizationPipeline = constructor(source=params.source,
                                                               destination=params.destination,
                                                               start_date=start_date,
                                                               end_date=end_date,
                                                               labels=labels,
                                                               )

        feed_pipeline.ensure_table_exists()
        feed_pipeline.clear_records()

        (
            self.pipeline
            | feed_pipeline.read_source()
            | feed_pipeline.normalize()
            | feed_pipeline.write_sink()
        )

    def run(self):
        return self.pipeline.run()
