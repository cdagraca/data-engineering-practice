import numpy as np
import pandas as pd
import re


class DataRow:
    def __init__(self, input_col_name, db_col_name, db_data_type):
        self.input_col_name = input_col_name
        self.db_col_name = db_col_name
        self.db_data_type = db_data_type

    def get_pd_rename_map(self):
        return {self.input_col_name: self.db_col_name}

    def get_db_col_def(self):
        return {self.db_col_name, self.db_data_type}


class InvalidLatLongFormatError(ValueError):
    def __init__(self, message):
        super(InvalidLatLongFormatError, self).__init__(f"Invalid lat-long value format: {message}")


def split_lat_long(row, input_col_name, lat_col_name, long_col_name, sep_char=" "):
    # TODO: handle NESW as well as +/-
    # TODO: mark errors instead of failing
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
    return row


