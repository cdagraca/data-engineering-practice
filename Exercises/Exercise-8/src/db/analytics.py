import duckdb

from duckdb.duckdb import DuckDBPyConnection, DuckDBPyRelation

class EVAnalytics:
    count_col_name = "count"
    rank_col_name = "count_rank"

    def __init__(self, db_conn: DuckDBPyConnection, table_name: str):
        self.db_conn = db_conn #duckdb.connect(db_name)
        self.table_name = table_name

    def group_and_count(self, columns: list):
        if len(columns) > 0:
            cols_string = ",".join(columns)
            return self.db_conn.sql(f"SELECT {cols_string}, count(*) AS {self.count_col_name} FROM {self.table_name} GROUP BY {cols_string}")
        else:
            raise ValueError("At least on grouping column must be specified")

    def rank_by_count(self, counts_data: DuckDBPyRelation, columns: list[str]):
        if len(columns) > 0:
            cols_string = ",".join(columns)
            partition_by = f"PARTITION BY {cols_string}"
        else:
            partition_by = ""
        window = f"over ({partition_by} ORDER BY {self.count_col_name} DESC) AS {self.rank_col_name}"

        return counts_data.rank(window, "*")

    def top_n(self, ranked_data: DuckDBPyRelation, n: int):
        return ranked_data.filter(f"{self.rank_col_name} <= {n}")
