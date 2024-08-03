import unittest
import duckdb
import pandas as pd

from db.analytics import *
from db.utils import *


class TestEVAnalytics(unittest.TestCase):

    data_no_ties = pd.DataFrame(data=[['UK', 'London', 'V&A'],
                                      ['UK', 'London', 'Natural History'],
                                      ['UK', 'London', 'Science'],
                                      ['UK', 'Edinburgh', 'National Galleries'],
                                      ['UK', 'Edinburgh', 'John Knox House'],
                                      ['France', 'Paris', 'Louvre'],
                                      ['France', 'Paris', 'Orsay'],
                                      ['France', 'Marseilles', 'Beaux-Art']],
                                columns=['country', 'city', 'museum'])
    table_name = "museums"

    def setUp(self):
        db_conn = DuckDBUtils(":memory:")
        schema = {
            "country": "VARCHAR",
            "city": "VARCHAR",
            "museum": "VARCHAR",
        }
        db_conn.create_table(self.table_name, schema)
        db_conn.load_data(self.data_no_ties, self.table_name)
        self.ev_analytics = EVAnalytics(db_conn.conn, self.table_name)

    @staticmethod
    def get_expected(data, columns, sort_by):
        return pd.DataFrame(data, columns=columns).sort_values(sort_by).to_dict("records")

    @staticmethod
    def format_result(data, sort_by):
        return data.to_df().sort_values(sort_by).to_dict("records")

    def test_group_and_count(self):
        expected = TestEVAnalytics.get_expected(
            [["UK", "London", 3],
             ["UK", "Edinburgh", 2],
             ["France", "Paris", 2],
             ["France", "Marseilles", 1]],
            ["country", "city", self.ev_analytics.count_col_name],
        ["country", "city"])
        self.assertEqual(
            TestEVAnalytics.format_result(self.ev_analytics.group_and_count(["country", "city"]),
                                          ["country", "city"]),
            expected
        )

    def test_count_no_group_error(self):
        with self.assertRaises(ValueError):
            self.ev_analytics.group_and_count([])

    def test_rank_by_count_with_partition(self):
        expected = TestEVAnalytics.get_expected(
            [["UK", "London", 3, 1],
             ["UK", "Edinburgh", 2, 2],
             ["France", "Paris", 2, 1],
             ["France", "Marseilles", 1, 2]],
            ["country", "city", self.ev_analytics.count_col_name, self.ev_analytics.rank_col_name],
        ["country", "city"])
        count_data = self.ev_analytics.group_and_count(["Country", "City"])
        self.assertEqual(
            TestEVAnalytics.format_result(self.ev_analytics.rank_by_count(count_data, ["country"]),
                                          ["country", "city"]),
            expected
        )

    def test_rank_by_count_no_partition(self):
        expected = TestEVAnalytics.get_expected(
            [["UK", "London", 3, 1],
             ["UK", "Edinburgh", 2, 2],
             ["France", "Paris", 2, 2],
             ["France", "Marseilles", 1, 4]],
            ["country", "city", self.ev_analytics.count_col_name, self.ev_analytics.rank_col_name],
        ["country", "city"])
        count_data = self.ev_analytics.group_and_count(["Country", "City"])
        self.assertEqual(
            TestEVAnalytics.format_result(self.ev_analytics.rank_by_count(count_data, []),
                                          ["country", "city"]),
            expected
        )

    def test_top_n_with_partition(self):
        expected = TestEVAnalytics.get_expected(
            [["UK", "London", 3, 1],
             ["France", "Paris", 2, 1]],
            ["country", "city", self.ev_analytics.count_col_name, self.ev_analytics.rank_col_name],
        ["country", "city"])
        count_data = self.ev_analytics.group_and_count(["Country", "City"])
        ranked_data = self.ev_analytics.rank_by_count(count_data, ["Country"])
        self.assertEqual(
            TestEVAnalytics.format_result(self.ev_analytics.top_n(ranked_data, 1),
                                          ["country", "city"]),
            expected
        )

    def test_rank_by_count_no_partition(self):
        expected = TestEVAnalytics.get_expected(
            [["UK", "London", 3, 1],
             ["UK", "Edinburgh", 2, 2],
             ["France", "Paris", 2, 2]],
            ["country", "city", self.ev_analytics.count_col_name, self.ev_analytics.rank_col_name],
        ["country", "city"])
        count_data = self.ev_analytics.group_and_count(["Country", "City"])
        ranked_data = self.ev_analytics.rank_by_count(count_data, [])
        self.assertEqual(
            TestEVAnalytics.format_result(self.ev_analytics.top_n(ranked_data, 2),
                                          ["country", "city"]),
            expected
        )




