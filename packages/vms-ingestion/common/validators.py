import argparse


def validate_country_name(country):
    valid_countries = [
        "blz",
        "bra",
        "chl",
        "cri",
        "ecu",
        "nor",
        "pan",
        "per",
        "png",
    ]

    if country not in valid_countries:
        raise argparse.ArgumentTypeError(f"Invalid country name {country}. Valid options are {valid_countries}")
    return country
