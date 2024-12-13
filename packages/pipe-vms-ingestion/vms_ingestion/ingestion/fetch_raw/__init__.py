import argparse
import json
from datetime import date, datetime, timezone

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from common.pipeline import build_pipeline_options_with_defaults
from common.transforms.map_api_ingest_to_position import MapAPIIngestToPosition
from common.transforms.map_naf_to_position import MapNAFToPosition
from common.transforms.read_json import ReadJson
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

    default_options = {"streaming": True, "save_main_session": True}

    options, beam_args = IngestionFetchRawOptions().parse_known_args()
    beam_options = PipelineOptions({**beam_args, **default_options})

    logging.info("Launching pipeline")

    process = "API_INGEST"

    with beam.Pipeline(options=beam_options) as pipeline:
        files = (
            pipeline
            | "Read from Pub/Sub" >> ReadFromPubSub(topic=options.input_topic, with_attributes=True)
            | 'Parse To JSON' >> beam.ParDo(FilterAndParse())
            | "Add common outout attributes" >> beam.Map(add_common_output_attributes)
        )
        naf_positions = (
            (
                files
                | "Filter NAF" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f == "NAF"))
                | "Read Lines from NAF File" >> ReadNAF(schema="gs://", error_topic=options.error_topic)
                | "Map NAF message to position" >> MapNAFToPosition()
            )
            if process == "NAF"
            else beam.Create([])
        )
        api_ingest_positions = (
            (
                files
                | "Filter API_INGEST" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f == "API_INGEST"))
                | "Read Lines from JSON File" >> ReadJson(schema="gs://", error_topic=options.error_topic)
                | "Map API_INGEST message to position" >> MapAPIIngestToPosition()
            )
            if process == "API_INGEST"
            else beam.Create([])
        )
        (
            (naf_positions + api_ingest_positions)
            | "Encode position" >> beam.ParDo(EncodePosition())
            | "Write to Pub/Sub" >> WriteToPubSub(topic=options.output_topic)
        )
        # unhandled positions
        (
            files
            | "Filter Unhandled" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f not in ["NAF", "API_INGEST"]))
            | "Write to Pub/Sub" >> WriteToPubSub(topic=options.error_topic)
        )
