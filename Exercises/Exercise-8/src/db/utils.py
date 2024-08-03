import duckdb
import pandas as pd

from duckdb.duckdb import CatalogException

VALID_DATA_TYPES = [
    "BIGINT",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "DATE",
    "DOUBLE",
    "FLOAT",
    "HUGEINT",
    "INTEGER",
    "INTERVAL",
    "SMALLINT",
    "SQLNULL",
    "TIME_TZ",
    "TIME",
    "TIMESTAMP_MS",
    "TIMESTAMP_NS",
    "TIMESTAMP_S",
    "TIMESTAMP_TZ",
    "TIMESTAMP",
    "TINYINT",
    "UBIGINT",
    "UHUGEINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    "UUID",
    "VARCHAR"
]


class DuckDBUtils:
    def __init__(self, db_name):
        self.conn = duckdb.connect(db_name)

    @staticmethod
    def format_schema(json_schema: dict):
        # TODO: handle decimal precision, scale; compound types
        invalid_dtypes = list(filter(lambda name_dtype: name_dtype[1].upper() not in VALID_DATA_TYPES,
                                     json_schema.items()))
        if len(invalid_dtypes) > 0:
            raise TypeError(f"One or more unsupported column data types specified")

        return ",".join(list(map(lambda name_dtype: f"{name_dtype[0]} {name_dtype[1].upper()}",
                                 json_schema.items())))

    def create_table(self, table_name: str, schema: dict, drop_if_exists: bool = False):
        formatted_schema = DuckDBUtils.format_schema(schema)
        if drop_if_exists:
            try:
                self.conn.execute(f"DROP TABLE {table_name}")
            except CatalogException as err:
                if f"Table with name {table_name} does not exist" in str(err):
                    pass
                else:
                    raise err
        self.conn.execute(f"CREATE TABLE {table_name} ({formatted_schema})")

    def load_data(self, data: pd.DataFrame, table_name: str):
        # TODO: consider checking column names, order, datatypes...
        my_data = data
        self.conn.sql(f"INSERT INTO {table_name} SELECT * FROM my_data")
