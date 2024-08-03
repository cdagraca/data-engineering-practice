import unittest

import duckdb
import pandas as pd

from duckdb.duckdb import CatalogException

from db.utils import *


class TestDbUtils(unittest.TestCase):
    def test_format_schema_happy(self):
        input_schema = {"col1": "VARCHAR", "col2": "INTEGER"}
        expected = "col1 VARCHAR,col2 INTEGER"
        assert DuckDBUtils.format_schema(input_schema) == expected

    def test_format_schema_invalid_dtype(self):
        input_schema = {"col1": "VARCHAR", "col2": "INVENTED"}
        with self.assertRaises(TypeError):
            DuckDBUtils.format_schema(input_schema)

    def test_create_table_happy(self):
        input_schema = {"col1": "VARCHAR", "col2": "INTEGER"}
        table_name = "my_table"
        db_conn = DuckDBUtils("")
        db_conn.create_table(table_name, input_schema)
        assert (db_conn.conn.execute(f"DESCRIBE {table_name}").df()[['column_name', 'column_type']].values.tolist() ==
                [['col1', 'VARCHAR'], ['col2', 'INTEGER']])

    def test_create_table_already_exists_err(self):
        input_schema = {"col1": "VARCHAR", "col2": "INTEGER"}
        table_name = "my_table_err"
        db_conn = DuckDBUtils("")
        db_conn.create_table(table_name, input_schema)
        with self.assertRaises(CatalogException):
            db_conn.create_table(table_name, input_schema, drop_if_exists=False)

    def test_create_table_already_exists_drop(self):
        input_schema = {"col1": "VARCHAR", "col2": "INTEGER"}
        table_name = "my_table_drop"
        db_conn = DuckDBUtils("")
        db_conn.create_table(table_name, input_schema, drop_if_exists=True)
        assert (db_conn.conn.execute(f"DESCRIBE {table_name}"
                                     ).df()[['column_name', 'column_type']].values.tolist() ==
                [['col1', 'VARCHAR'], ['col2', 'INTEGER']])

    def test_load_data_happy(self):
        input_schema = {"col1": "VARCHAR", "col2": "INTEGER"}
        table_name = "my_table"
        db_conn = DuckDBUtils("")
        db_conn.create_table(table_name, input_schema)
        data = pd.DataFrame.from_dict({"col1": ["a", "b"], "col2": [1, 2]})
        db_conn.load_data(data, table_name)
        assert db_conn.conn.sql(f"SELECT * FROM {table_name}").df().to_dict() == data.to_dict()


