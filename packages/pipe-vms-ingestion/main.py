import sys

from vms_ingestion.normalization import run_normalization

from logger import logger

logger.setup_logger(1)
logging = logger.get_logger()


SUBCOMMANDS = {
    "normalize": run_normalization,
}

if __name__ == "__main__":
    logging.info("Running %s", sys.argv)

    if len(sys.argv) < 2:
        logging.info(
            "No subcommand specified. Run pipeline [SUBCOMMAND], " + "where subcommand is one of %s", SUBCOMMANDS.keys()
        )
        exit(1)

    subcommand = sys.argv[1]
    subcommand_args = sys.argv[2:]

    SUBCOMMANDS[subcommand](subcommand_args)
