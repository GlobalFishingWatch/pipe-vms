import csv
import json
import shutil

import apache_beam as beam
import icdiff
from pprintpp import pformat

AUTO_COLS = shutil.get_terminal_size().columns
MARGIN_L = 10
GUTTER = 2
MARGINS = MARGIN_L + GUTTER + 1
ICDIFF_COLS = 200
ICDIFF_TABSIZE = 2
ICDIFF_SHOW_ALL_SPACES = False
ICDIFF_HIGHLIGHT = False
ICDIFF_LINE_NUMBERS = False
ICDIFF_TRUNCATE = False
ICDIFF_STRIP_TRAILING_CR = False

# Based on https://github.com/hjwp/pytest-icdiff/blob/main/pytest_icdiff.py
# but adapted to use with apache beam assertions


def read_csv_to_list(file_path):
    data_list = []
    with open(file_path, mode="r", newline="") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            data_list.append(row)
    return data_list


def read_json(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def diff_dicts(left, right):
    try:
        if abs(left + right) < 19999:
            return
    except TypeError:
        pass
    except ValueError:
        # ValueErrors are raised when numpy / pandas errors are checked
        # Bail out of generating a diff and use pytest default output
        return

    COLS = int(ICDIFF_COLS or AUTO_COLS)
    half_cols = COLS / 2 - MARGINS
    TABSIZE = int(ICDIFF_TABSIZE or 2)

    if isinstance(left, str) and isinstance(right, str):
        pretty_left = left.splitlines()
        pretty_right = right.splitlines()
    else:
        pretty_left = pformat(left, indent=TABSIZE, width=half_cols).splitlines()
        pretty_right = pformat(right, indent=TABSIZE, width=half_cols).splitlines()
    diff_cols = COLS - MARGINS

    if len(pretty_left) < 3 or len(pretty_right) < 3:
        # avoid small diffs far apart by smooshing them up to the left
        smallest_left = pformat(left, indent=TABSIZE, width=1).splitlines()
        smallest_right = pformat(right, indent=TABSIZE, width=1).splitlines()
        max_side = max(len(ln) + 1 for ln in smallest_left + smallest_right)
        if (max_side * 2 + MARGINS) < COLS:
            diff_cols = max_side * 2 + GUTTER
            pretty_left = pformat(left, indent=TABSIZE, width=max_side).splitlines()
            pretty_right = pformat(right, indent=TABSIZE, width=max_side).splitlines()

    differ = icdiff.ConsoleDiff(
        cols=diff_cols,
        show_all_spaces=ICDIFF_SHOW_ALL_SPACES,
        highlight=ICDIFF_HIGHLIGHT,
        line_numbers=ICDIFF_LINE_NUMBERS,
        tabsize=TABSIZE,
        truncate=ICDIFF_TRUNCATE,
        strip_trailing_cr=ICDIFF_STRIP_TRAILING_CR,
    )

    if False:
        # colorization is disabled in Pytest - either due to the terminal not
        # supporting it or the user disabling it. We should obey, but there is
        # no option in icdiff to disable it, so we replace its colorization
        # function with a no-op
        differ.colorize = lambda string: string
        color_off = ""
    else:
        color_off = icdiff.color_codes["none"]

    icdiff_lines = list(differ.make_table(pretty_left, pretty_right))
    if len(icdiff_lines) > 50:
        icdiff_lines = list(differ.make_table(pretty_left, pretty_right, context=True))

    return ["equals failed"] + [color_off + line for line in icdiff_lines]


class CustomBeamAssertException(Exception):
    """Exception raised by matcher classes used by assert_that transform."""

    pass


# This is a customization of apache_beam.testing.util.equal_to that improves assert
# failure output by printing a diff of the two objects


def pcol_equal_to(expected, equals_fn=None):
    def _equal(actual, equals_fn=equals_fn):
        expected_list = list(expected)

        # Try to compare actual and expected by sorting. This fails with a
        # TypeError in Python 3 if different types are present in the same
        # collection. It can also raise false negatives for types that don't have
        # a deterministic sort order, like pyarrow Tables as of 0.14.1
        if not equals_fn:

            def equals_fn(e, a):
                return e == a

            try:
                sorted_expected = sorted(expected)
                sorted_actual = sorted(actual)
                if sorted_expected == sorted_actual:
                    return
            except TypeError:
                pass
        # Slower method, used in two cases:
        # 1) If sorted expected != actual, use this method to verify the inequality.
        #    This ensures we don't raise any false negatives for types that don't
        #    have a deterministic sort order.
        # 2) As a fallback if we encounter a TypeError in python 3. this method
        #    works on collections that have different types.
        unexpected = []

        # every element in the collection
        for element in actual:
            found = False
            for i, v in enumerate(expected_list):
                # checks equality
                if equals_fn(v, element):
                    found = True
                    expected_list.pop(i)
                    break

            if not found:
                unexpected.append(element)

        if unexpected or expected_list:
            diff_lines = diff_dicts(expected_list, actual)
            msg = "\n".join(diff_lines)
            # msg = msg + f'\n os.get_terminal_size().columns = {AUTO_COLS}'
            raise CustomBeamAssertException(msg)

    return _equal


class MockTransform(beam.PTransform):
    def expand(self, pcoll):
        return pcoll
