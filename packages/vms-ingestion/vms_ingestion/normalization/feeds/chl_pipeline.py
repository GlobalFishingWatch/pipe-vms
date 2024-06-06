from hashlib import md5

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

    def get_source_ssvid(self, msg):
        # Current ssvid TO_HEX(MD5(shipname)) as ssvid,
        return msg['shipname']
        # return md5(msg['shipname'].encode('utf-8')).hexdigest()

    def mapToNormalizedFeedSpecificMessage(self, msg):
        result = {
            **msg,
            **{
                "source": f"chile_vms_{msg['source_fleet']}",
                "source_provider": 'SERNAPESCA',
            }
        }
        return result
