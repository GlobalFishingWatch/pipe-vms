from datetime import datetime
import argparse


def check_if_a_key_exists_in_a_dic(keys, dictionary):
    for key in keys:
        try:
            dictionary[key]
        except KeyError:
            print(f'{key} is required')
            exit()


def check_if_it_is_a_valid_YYYYMMDD(date):
    try:
        datetime.strptime(date, "%Y%m%d")
        return date
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(date)
        raise argparse.ArgumentTypeError(msg)
