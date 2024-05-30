import apache_beam as beam
from vms_ingestion.normalization.feed_normalization_pipeline import \
    FeedNormalizationPipeline


class CHLFeedPipeline(FeedNormalizationPipeline):

    def __init__(self,
                 source,
                 destination,
                 start_date,
                 end_date,
                 labels) -> None:
        super().__init__(source,
                         destination,
                         start_date,
                         end_date,
                         labels,
                         feed='chl')

    def mapToNormalizedFeedSpecificMessage(self, msg):
        result = {
            **msg,
            **{
                "source_provider": 'SERNAPESCA',
            }
        }
        return result

    class Normalize(beam.PTransform):
        def __init__(self, pipe, label=None) -> None:
            super().__init__(label)
            self.pipe = pipe

        def expand(self, pcoll):
            return (
                pcoll
                | beam.Map(self.pipe.mapToNormalizedMessage)
                | beam.Map(self.pipe.mapToNormalizedFeedSpecificMessage)
            )
