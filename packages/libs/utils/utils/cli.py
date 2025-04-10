from logger import logger

logging = logger.get_logger()


def execute_subcommand(
    args, subcommands, missing_subcomand_message="No subcommand specified for command. Run pipeline [SUBCOMMAND]"
):

    if len(args) < 1:
        logging.error(
            missing_subcomand_message + ", where subcommand is one of \n%s",
            "\n".join([f"    {item}" for item in list(subcommands.keys())]),
        )
        exit(1)

    subcommand = args[0]

    if subcommand not in subcommands:
        logging.error(
            f"Invalid subcommand [{subcommand}] provided. Subcommand should be one of the following "
            f"{list(subcommands.keys())}"
        )
        exit(1)

    subcommand_args = args[1:]

    subcommands[subcommand](subcommand_args)
