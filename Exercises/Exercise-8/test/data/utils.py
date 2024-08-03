import unittest

import numpy as np
import pandas as pd

from data.utils import *


class TestDataUtils(unittest.TestCase):

    def run_split_lat_long_success_test(self, input, sep_char, expected):
        # replacing nan with None is a bit of a cheat for easy testing
        assert (
            split_lat_long(input, "latlong", "lat", "long", sep_char
                           ).replace({np.nan: None}).to_dict() ==
            expected.replace({np.nan: None}).to_dict()
        )

    def test_split_lat_long_simple_comma(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"{lat},{long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat","long"])
        self.run_split_lat_long_success_test(row, ",", expected)

    def test_split_lat_long_comma_with_spaces(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"{lat} , {long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat","long"])
        self.run_split_lat_long_success_test(row, ",", expected)

    def test_split_lat_long_nan(self):
        lat_long_string = np.nan
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, np.nan, np.nan], ["id", "latlong", "lat","long"])
        self.run_split_lat_long_success_test(row, ",", expected)

    def test_split_lat_long_simple_space(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"{lat} {long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat","long"])
        self.run_split_lat_long_success_test(row, " ", expected)

    def test_split_lat_long_multi_space(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f" {lat}  {long}"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat","long"])
        self.run_split_lat_long_success_test(row, " ", expected)

    def test_split_lat_long_point(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"POINT ({lat} {long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        expected = pd.Series([1, lat_long_string, float(lat), float(long)], ["id", "latlong", "lat","long"])
        self.run_split_lat_long_success_test(row, " ", expected)

    def test_split_lat_long_invalid_number(self):
        lat = "0.002a"
        long = "44.201"
        lat_long_string = f"POINT ({lat} {long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        with self.assertRaises(InvalidLatLongFormatError):
            split_lat_long(row, "latlong", "lat", "long", " ")

    def test_split_lat_long_no_separator(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"POINT ({lat}{long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        with self.assertRaises(InvalidLatLongFormatError):
            split_lat_long(row, "latlong", "lat", "long", " ")

    def test_split_lat_long_unmatched_parentheses(self):
        lat = "0.002"
        long = "44.201"
        lat_long_string = f"POINT {lat} {long})"
        row = pd.Series([1, lat_long_string], ["id", "latlong"])
        with self.assertRaises(InvalidLatLongFormatError):
            split_lat_long(row, "latlong", "lat", "long", " ")
