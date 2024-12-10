import apache_beam as beam
import six
from apache_beam import typehints
from logger import logger

logger.setup_logger(1)
logging = logger.get_logger()


class NAFCoder(beam.coders.Coder):
    """A coder used for reading and writing NAF"""

    def encode(self, value: dict):
        fields = []
        for k, v in value.items():
            if isinstance(v, str):
                fields.append(f'{k.upper()}/{v.strip().encode("utf-8")}')
            elif isinstance(v, list):
                fields.append(f'{k.upper()}/{",".join(v).encode("utf-8")}')
            elif isinstance(v, dict):
                fields.append(f'{k.upper()}/{",".join(v.values().encode("utf-8"))}')
            else:
                fields.append(f'{k.upper()}/{str(v).encode("utf-8")}')

        return f'//SR/{"/".join(fields)}//ER//'

    def decode(self, value):
        stripped_line = value.decode('UTF-8').strip()
        try:
            if len(stripped_line.split('///')) > 1:
                logging.warning(f"There are empty fields in line {stripped_line}")
                splitted = stripped_line.rsplit('//')
            else:
                splitted = stripped_line.split('//')
            return self.parse(splitted)

        except Exception as err:
            logging.error(err)
            logging.error("Unable to convert NAF message to dict {}".format(stripped_line))
            # if it is not valid just exclude it.
            raise err

    """
    Parses the NAF message.
    """

    def parse(self, entries):
        start = False
        end = False
        # header = []
        row = {}
        for entry in entries:
            pair = entry.split('/')
            if pair[0] == 'ER':
                end = True
            if start and not end:
                label = pair[0]
                # header.append(label)
                row[label] = self.normalize_value(label, pair[1])
            if pair[0] == 'SR':
                start = True
        return row

    """
    Normalizes the value.

    :@param label: The label or column name.
    :@type label: str.
    :@param value: The value of the row.
    :@type value: str.
    :@return: the normalized value.
    """

    def normalize_value(self, label, value):
        return value

    def is_deterministic(self):
        return True


JSONDict = typehints.Dict[six.binary_type, typehints.Any]
