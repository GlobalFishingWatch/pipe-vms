import json
from datetime import date, datetime, timezone

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from common.pipeline import build_pipeline_options_with_defaults
from common.transforms.map_naf_to_position import MapNAFToPosition
from common.transforms.read_naf import ReadNAF
from logger import logger
from vms_ingestion.ingestion.fetch_raw.options import IngestionFetchRawOptions
from vms_ingestion.ingestion.fetch_raw.transforms.filter import FilterAndParse, FilterFormat

logger.setup_logger(1)
logging = logger.get_logger()


class EncodePosition(beam.DoFn):
    def process(self, message):
        yield json.dumps(message, default=default_serializer).encode('utf-8')


def default_serializer(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()

    return str(o)


def add_common_output_attributes(message):
    """Add common output attributes to the message."""
    message['common'] = {
        **message['attributes'],
        "received_at": datetime.fromisoformat(message['timeCreated'].replace("T", " ").replace("Z", "")).replace(
            tzinfo=timezone.utc
        ),
    }
    return message


def fetch_raw(argv):

    logging.info("Running fetch raw ingestion dataflow pipeline with args %s", argv)

    logging.info("Building pipeline options")
    options = build_pipeline_options_with_defaults(argv, **{"streaming": True, "save_main_session": True}).view_as(
        IngestionFetchRawOptions
    )

    logging.info("Launching pipeline")

    input_topic = options.input_topic  # ."gfw-raw-data-vms-central-notification-topic"
    with beam.Pipeline(options=options) as pipeline:
        files = (
            pipeline
            | "Read from Pub/Sub" >> ReadFromPubSub(topic=input_topic, with_attributes=True)
            | 'Parse To JSON' >> beam.ParDo(FilterAndParse())
            | "Add common outout attributes" >> beam.Map(add_common_output_attributes)
        )
        naf_positions = (
            files
            | "Filter NAF" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f == "NAF"))
            | "Read Lines from NAF File" >> ReadNAF(schema="gs://", error_topic=options.error_topic)
            | "Map NAF message to position" >> MapNAFToPosition()
        )
        (
            naf_positions
            | "Encode position" >> beam.ParDo(EncodePosition())
            | "Write to Pub/Sub" >> WriteToPubSub(topic=options.output_topic)
        )
        # unhandled positions
        (
            files
            | "Filter Unhandled" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f != "NAF"))
            | "Write to Pub/Sub" >> WriteToPubSub(topic=options.error_topic)
        )
