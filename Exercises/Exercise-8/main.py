import os.path

from src.data.utils import *
from src.db.analytics import *
from src.db.utils import *

db_schema = {
    "VIN_1_10": "VARCHAR",
    "county": "VARCHAR",
    "city": "VARCHAR",
    "state": "VARCHAR",
    "postal_code": "BIGINT",
    "model_year": "SMALLINT",
    "make": "VARCHAR",
    "model": "VARCHAR",
    "EV_type":  "VARCHAR",
    "CAFV_eligibility": "VARCHAR",
    "electric_range": "SMALLINT",
    "base_MSRP": "INTEGER",
    "legislative_district": "FLOAT",
    "DOL_vehicle_ID": "BIGINT",
    "electric_utility": "VARCHAR",
    "census_tract_2020": "BIGINT",
    "vehicle_location_lat": "FLOAT",
    "vehicle_location_long": "FLOAT",
}

err_db_schema = dict(zip(db_schema.keys(), ["varchar"]*len(db_schema.keys())))
err_db_schema["errors"] = "VARCHAR"

db_name = "ex8.db"
table_name = "ev_population_data"
err_table_name = "ev_population_errors"
data_path = "/app/data"
output_path = f"{data_path}/output"


def main():
    data = pd.read_csv(f"{data_path}/Electric_Vehicle_Population_Data.csv")
    data = data.apply(lambda row: LatLongSplitter("Vehicle Location", "Vehicle Location Lat", "Vehicle Location Long"
                                                  ).process_value(row), axis=1)
    data.drop("Vehicle Location", axis=1, inplace=True)
    for int_col in [kv[0] for kv in db_schema.keys() if kv[1] in ["INTEGER", "BIGINT", "SMALLINT"]]:
        data = data.apply(lambda row: CheckInt(int_col).process_value(row))
    for float_col in [kv[0] for kv in db_schema.keys() if kv[1] in ["FLOAT", "DOUBLE"]]:
        data = data.apply(lambda row: CheckFloat(float_col).process_value(row))
    conn = DuckDBUtils(db_name)
    conn.create_table(table_name, db_schema, drop_if_exists=True)
    # create an exceptions table of all-strings to hold records that fail processing for any reason
    conn.create_table(err_table_name, err_db_schema,
                      drop_if_exists=True)
    conn.load_data(data, table_name, err_table_name)

    analyser = EVAnalytics(conn.conn, table_name)

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    # Output 1: count number of electric cars by city
    vehicles_by_city = analyser.group_and_count(["state", "county", "city"])
    vehicles_by_city.sort(["state", "county", "city"]).to_csv(f"{output_path}/vehicles_by_city.csv")

    # Output 2: top 3 most popular EVs
    vehicles_by_make_model = analyser.group_and_count(["make", "model"])
    vehicle_make_model_ranks = analyser.rank_by_count(vehicles_by_make_model, [])
    top_3_vehicles = analyser.top_n(vehicle_make_model_ranks, 3)
    top_3_vehicles.to_csv(f"{output_path}/top_3_vehicles.csv")

    # Output 3: top vehicle by postal code
    vehicle_types_by_postal_code = analyser.group_and_count(["postal_code", "make", "model"])
    vehicle_ranks_by_postal_code = analyser.rank_by_count(vehicle_types_by_postal_code, ["postal_code"])
    top_by_postal_code = analyser.top_n(vehicle_ranks_by_postal_code, 1)
    top_by_postal_code.sort(["postal_code", EVAnalytics.rank_col_name]
                            ).to_csv(f"{output_path}/top_vehicle_by_postal_code.csv")

    # Output 4: count by model year, write partitions
    counts_by_year = analyser.group_and_count(["make", "model", "model_year"])
    # TODO: convert to spark and just use spark_df.write.partitionBy("model_year").parquet(...)
    # apparently there is an "experimental" API where "features are still missing"
    conn.conn.execute(f"COPY counts_by_year TO '{output_path}/counts_by_model_year.parquet' " +
                      "(FORMAT parquet, PARTITION_BY (model_year), OVERWRITE_OR_IGNORE)")


if __name__ == "__main__":
    main()
