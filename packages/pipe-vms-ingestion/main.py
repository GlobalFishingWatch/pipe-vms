import sys
from logging import NOTSET

from logger import logger
from utils.cli import execute_subcommand
from vms_ingestion.ingestion import run_ingestion
from vms_ingestion.normalization import run_normalization
from vms_ingestion.utils import run_utils

logger.setup_logger(NOTSET)
logging = logger.get_logger()


SUBCOMMANDS = {
    "normalize": run_normalization,
    "ingestion": run_ingestion,
    "utils": run_utils,
}

if __name__ == "__main__":
    logging.info("Running %s", sys.argv)
    args = sys.argv[1:]
    execute_subcommand(args=args, subcommands=SUBCOMMANDS)
