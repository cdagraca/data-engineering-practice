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

    def load_data(self, data: pd.DataFrame, table_name: str, err_table_name: str, err_column_name: str = "errors"):
        # TODO: consider checking column names, order
        # TODO: does duckdb support diverting failed records to error table?
        select_clause = "*"
        where_clause = ""
        if "errors" in data.columns:
            self.conn.sql(f"INSERT INTO {err_table_name} SELECT * FROM data WHERE {err_column_name} IS NOT NULL")
            select_clause = ",".join([c for c in data.columns if c != err_column_name])
            where_clause = f"WHERE {err_column_name} IS NULL"
        self.conn.sql(f"INSERT INTO {table_name} SELECT {select_clause} FROM my_data {where_clause}")
