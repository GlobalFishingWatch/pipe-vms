import argparse


class IngestionFetchRawOptions:
    def __init__(self):
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--input_topic",
            required=True,
            help="The Cloud Pub/Sub topic to read from.",
        )

        parser.add_argument(
            "--output_topic",
            required=True,
            help="The Cloud Pub/Sub topic to write positions to.",
        )

        parser.add_argument(
            "--error_topic",
            required=True,
            help="The Cloud Pub/Sub topic to write failed messages to.",
        )
        self.parser = parser

    def parse_known_args(self, **argv):
        return self.parser.parse_known_args(**argv)
