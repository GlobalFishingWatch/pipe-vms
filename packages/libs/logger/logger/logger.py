import logging
import os
import sys


def setup_logger(verbosity):
    base_loglevel = getattr(
        logging,
        (os.getenv('LOGLEVEL', 'WARNING')).upper()
    )

    verbosity = min(verbosity, 2)

    loglevel = base_loglevel - (verbosity * 10)

    logging.basicConfig(
        stream=sys.stdout,
        level=loglevel,
        format='%(message)s'
    )


def get_logger():
    return logging.getLogger()
