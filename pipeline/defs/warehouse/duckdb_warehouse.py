
import os
from dagster import asset, AssetExecutionContext
from pipeline.constants import WAREHOUSE_PATH
from pathlib import Path

from pipeline.constants import (
    SINGLE_PATH_ASSETS_PATHS,
    PARTITIONED_ASSETS_PATHS,
    WAREHOUSE_PATH,
)
from pipeline.utils.duckdb_wrapper import DuckDBWrapper

@asset(
    deps=[
        "mta_subway_hourly_ridership",
        "mta_daily_ridership",
        "mta_operations_statement",
        "daily_weather_asset",
        "hourly_weather_asset",
    ],
    compute_kind="DuckDB",
    group_name="Warehouse",
)
def duckdb_warehouse(context: AssetExecutionContext):
    """
    Creates a persistent DuckDB file at WAREHOUSE_PATH and registers each
    asset as a DuckDB view. Partitioned assets use a different method.
    Creates the directory and database if they don't exist, and replaces
    any existing DuckDB file with a new one.
    """

    # Ensure WAREHOUSE_PATH directory exists
    warehouse_dir = Path(WAREHOUSE_PATH).parent
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    # If a DuckDB file exists at WAREHOUSE_PATH, delete it
    if os.path.exists(WAREHOUSE_PATH):
        os.remove(WAREHOUSE_PATH)
        context.log.info(f"Existing DuckDB file at {WAREHOUSE_PATH} deleted.")

    # Create new DuckDB connection (this will create a new file)
    duckdb_wrapper = DuckDBWrapper(WAREHOUSE_PATH)
    context.log.info(f"New DuckDB file created at {WAREHOUSE_PATH}")

    # Register non-partitioned assets (MTA, OTHER_MTA, WEATHER)
    non_partitioned = {**SINGLE_PATH_ASSETS_PATHS}
    if non_partitioned:
        table_names = list(non_partitioned.keys())
        repo_root = os.path.dirname(WAREHOUSE_PATH)  # Use the warehouse's parent dir as repo_root
        base_path = os.path.relpath(os.path.dirname(list(non_partitioned.values())[0]), repo_root)
        duckdb_wrapper.bulk_register_data(
            repo_root=repo_root,
            base_path=base_path,
            table_names=table_names,
            wildcard="*.parquet",
            as_table=False,  # Register as views
            show_tables=False
        )

    # Register partitioned assets
    partitioned = {**PARTITIONED_ASSETS_PATHS}
    if partitioned:
        table_names = list(partitioned.keys())
        repo_root = os.path.dirname(WAREHOUSE_PATH)  # Use the warehouse's parent dir as repo_root
        base_path = os.path.relpath(os.path.dirname(list(partitioned.values())[0]), repo_root)
        duckdb_wrapper.bulk_register_partitioned_data(
            repo_root=repo_root,
            base_path=base_path,
            table_names=table_names,
            wildcard="year=*/month=*/*.parquet",
            as_table=False,  # Register as views
            show_tables=False  # Optional: show tables for verification
        )

    duckdb_wrapper.con.close()
    context.log.info("Connection to DuckDB closed.")
    return None