import json
from datetime import date, datetime

import apache_beam as beam
from logger import logger

logger.setup_logger(1)
logging = logger.get_logger()


class JSONCoder(beam.coders.Coder):
    """A coder used for reading and writing JSON"""

    def encode(self, value: dict):
        try:
            return json.dumps(value).encode('utf-8')

        except Exception as err:
            logging.error(err)
            logging.error("Unable to convert dict object to JSON message {}".format(value))
            raise err

    def decode(self, value):
        try:
            stripped_line = value.decode('utf-8').strip()
            return json.loads(stripped_line)

        except Exception as err:
            logging.error(err)
            logging.error("Unable to convert JSON message to dict {}".format(stripped_line))
            # if it is not valid just exclude it.
            raise err

    def is_deterministic(self):
        return True


def default_serializer(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()

    return str(o)
