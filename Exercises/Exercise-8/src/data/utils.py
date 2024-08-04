import numpy as np
import pandas as pd
import re

from abc import ABC, abstractmethod


class InvalidLatLongFormatError(ValueError):

    err_str = "Unsupported or invalid lat-long value format: %s"

    def __init__(self, message):
        super(InvalidLatLongFormatError, self).__init__(self.err_str.format(message))


class ValueProcessor(ABC):

    @abstractmethod
    def __process_value__(self, row):
        pass

    def process_value(self, row):
        try:
            self.__process_value__(row)
        except Exception as err:
            if "errors" in row.keys():
                row["errors"] = row["errors"] + f";{str(err)}"
            else:
                row["errors"] = str(err)
        return row


class LatLongSplitter(ValueProcessor):
    def __init__(self, input_col_name, lat_col_name, long_col_name, sep_char=" "):
        self.input_col_name = input_col_name
        self.lat_col_name = lat_col_name
        self.long_col_name = long_col_name
        self.sep_char = sep_char

    def __process_value__(self, row):
        # TODO: handle NESW as well as +/-
        row[self.lat_col_name] = np.nan
        row[self.long_col_name] = np.nan
        if not pd.isna(row[self.input_col_name]):
            lat_long = row[self.input_col_name]
            lat_long = re.sub(r"\s+", " ", lat_long)
            if '(' in lat_long:
                if ')' not in lat_long or lat_long.index(')') < lat_long.index('('):
                    raise InvalidLatLongFormatError(row[self.input_col_name])
                lat_long = lat_long[lat_long.index('(')+1:lat_long.index(')')]
            lat_long = lat_long.strip()
            if lat_long.strip().count(self.sep_char) != 1:
                raise InvalidLatLongFormatError(row[self.input_col_name])
            try:
                parts = lat_long.split(self.sep_char)
                row[self.lat_col_name] = float(parts[0].strip())
                row[self.long_col_name] = float(parts[1].strip())
            except ValueError:
                raise InvalidLatLongFormatError(row[self.input_col_name])


class CheckInt(ValueProcessor):
    def __init__(self, col_name):
        self.col_name = col_name

    def __process_value__(self, row):
        # TODO: enhanced checks for different int lengths
        int(row[self.col_name])


class CheckFloat(ValueProcessor):
    def __init__(self, col_name):
        self.col_name = col_name

    def __process_value__(self, row):
        # TODO: enhanced checks for float/double
        float(row[self.col_name])


def split_lat_long(row, input_col_name, lat_col_name, long_col_name, sep_char=" "):
    # TODO: handle NESW as well as +/-
    # TODO: mark errors instead of failing
    try:
        if not pd.isna(row[input_col_name]):
            lat_long = row[input_col_name]
            lat_long = re.sub(r"\s+", " ", lat_long)
            if '(' in lat_long:
                if ')' not in lat_long or lat_long.index(')') < lat_long.index('('):
                    raise InvalidLatLongFormatError(row[input_col_name])
                lat_long = lat_long[lat_long.index('(')+1:lat_long.index(')')]
            lat_long = lat_long.strip()
            if lat_long.strip().count(sep_char) != 1:
                raise InvalidLatLongFormatError(row[input_col_name])
            try:
                parts = lat_long.split(sep_char)
                row[lat_col_name] = float(parts[0].strip())
                row[long_col_name] = float(parts[1].strip())
            except ValueError:
                raise InvalidLatLongFormatError(row[input_col_name])
        else:
            row[lat_col_name] = np.nan
            row[long_col_name] = np.nan
    except InvalidLatLongFormatError as err:
        if "errors" in row.keys():
            row["errors"] = row["errors"] + f";{str(err)}"
        else:
            row["errors"] = str(err)
    return row


