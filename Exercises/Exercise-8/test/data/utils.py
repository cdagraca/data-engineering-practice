import unittest

import numpy as np
import pandas as pd

from data.utils import *


def dict_compare(actual, expected):
    # replacing nan with None is a bit of a cheat for easy testing
    assert actual.replace({np.nan: None}).to_dict() == expected.replace({np.nan: None}).to_dict()


def run_split_lat_long_test(input_row, sep_char, expected):
    latlong_processor = LatLongSplitter("latlong", "lat", "long", sep_char)
    dict_compare(latlong_processor.process_value(input_row), expected.replace({np.nan: None}))


def run_check_int_test(input_row, expected):
    int_checker = CheckInt("int_col")
    dict_compare(int_checker.process_value(input_row), expected)


def run_check_float_test(input_row, expected):
    float_checker = CheckFloat("float_col")
    dict_compare(float_checker.process_value(input_row), expected)


class TestChecker(ValueProcessor):
    def __init__(self, should_fail: bool):
        self.should_fail = should_fail

    def __process_value__(self, row):
        if self.should_fail:
            raise RuntimeError("uh-oh")


class TestDataUtils(unittest.TestCase):

    def test_abstract_success(self):
        row = pd.Series([1, "a"], ["id", "val"])
        expected = row
        assert (TestChecker(should_fail=False).process_value(row).to_dict() == expected.to_dict())

    def test_abstract_success_prev_err(self):
        row = pd.Series([1, "a", "o noes!"], ["id", "val", "errors"])
        expected = row
        assert (TestChecker(should_fail=False).process_value(row).to_dict() == expected.to_dict())

    def test_abstract_fail(self):
        row = pd.Series([1, "a", "o noes!"], ["id", "val", "errors"])
        expected = pd.Series([1, "a", "o noes!;uh-oh"], ["id", "val", "errors"])
        assert (TestChecker(should_fail=True).process_value(row).to_dict() == expected.to_dict())

    def test_abstract_fail_prev_err(self):
        row = pd.Series([1, "a"], ["id", "val"])
        expected = pd.Series([1, "a", "uh-oh"], ["id", "val", "errors"])
        assert (TestChecker(should_fail=True).process_value(row).to_dict() == expected.to_dict())

    def test_split_lat_long_simple_comma(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"{lat},{long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat", "long"])
        run_split_lat_long_test(row, ",", expected)

    def test_split_lat_long_comma_with_spaces(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"{lat} , {long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat", "long"])
        run_split_lat_long_test(row, ",", expected)

    def test_split_lat_long_nan(self):
        lat_long_string = np.nan
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, np.nan, np.nan], ["id", "latlong", "lat", "long"])
        run_split_lat_long_test(row, ",", expected)

    def test_split_lat_long_simple_space(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"{lat} {long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat", "long"])
        run_split_lat_long_test(row, " ", expected)

    def test_split_lat_long_multi_space(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f" {lat}  {long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat", "long"])
        run_split_lat_long_test(row, " ", expected)

    def test_split_lat_long_point(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"POINT ({lat} {long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat", "long"])
        run_split_lat_long_test(row, " ", expected)

    def test_split_lat_long_invalid_number(self):
        lat = "0.002a"
        long = "44.201"
        lat_long_string = f"POINT ({lat} {long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, np.nan, np.nan,
                              InvalidLatLongFormatError.err_str.format(lat_long_string)],
                             ["id", "latlong", "lat", "long", "errors"])
        run_split_lat_long_test(row, " ", expected)

    def test_split_lat_long_no_separator(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"POINT ({lat}{long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, np.nan, np.nan,
                              InvalidLatLongFormatError.err_str.format(lat_long_string)],
                             ["id", "latlong", "lat", "long", "errors"])
        run_split_lat_long_test(row, " ", expected)

    def test_split_lat_long_unmatched_parentheses(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"POINT {lat} {long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, np.nan, np.nan,
                              InvalidLatLongFormatError.err_str.format(lat_long_string)],
                             ["id", "latlong", "lat", "long", "errors"])
        run_split_lat_long_test(row, " ", expected)

    def test_split_lat_long_partial_success(self):
        lat = "0.002"
        long = "44.201a"
        lat_long_string = f"POINT ({lat} {long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), np.nan,
                              InvalidLatLongFormatError.err_str.format(lat_long_string)],
                             ["id", "latlong", "lat", "long", "errors"])
        run_split_lat_long_test(row, " ", expected)

    def test_check_int_happy(self):
        int_val = "1"
        row = pd.Series([1, int_val], ["id", "int_col"])
        expected = row
        run_check_int_test(row, expected)

    def test_check_int_fail(self):
        int_val = "a"
        err_val = "invalid literal for int() with base 10: 'a'"
        row = pd.Series([1, int_val], ["id", "int_col"])
        expected = pd.Series([1, int_val, err_val], ["id", "int_col", "errors"])
        run_check_int_test(row, expected)

    def test_check_float_happy(self):
        float_val = "1.1"
        row = pd.Series([1, float_val], ["id", "float_col"])
        expected = row
        run_check_float_test(row, expected)

    def test_check_float_fail(self):
        float_val = "a"
        err_val = "could not convert string to float: 'a'"
        row = pd.Series([1, float_val], ["id", "float_col"])
        expected = pd.Series([1, float_val, err_val], ["id", "float_col", "errors"])
        run_check_float_test(row, expected)
