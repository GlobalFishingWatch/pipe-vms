import json

import apache_beam as beam
from logger import logger

logger.setup_logger(1)
logging = logger.get_logger()


class FilterAndParse(beam.DoFn):
    """Filter and parse the message to json"""

    def process(self, message):
        if message.attributes["eventType"] == "OBJECT_FINALIZE":
            data = json.loads(message.data.decode('utf-8'))
            # Attach the attributes to the data
            data['attributes'] = message.attributes
            yield data
        else:
            logging.info("Skip message does not correspond to a file creation")


class FilterFormat(beam.DoFn):
    """Filter the message by format"""

    def __init__(self, filter_format_fn=None):
        self.filter_format_fn = filter_format_fn

    def process(self, message):
        if self.filter_format_fn(message['attributes']["format"]):
            yield message
