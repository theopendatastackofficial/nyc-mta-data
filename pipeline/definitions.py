import os
import dagster as dg
import dagster_components as dg_components

# Import resources and configuration dependencies
from pipeline.resources.io_managers.single_file_polars_parquet_io_manager import SingleFilePolarsParquetIOManager
from pipeline.resources.io_managers.fastopendata_partitioned_parquet_io_manager import FastOpenDataPartitionedParquetIOManager
from pipeline.resources.socrata_resource import SocrataResource
from dagster_dbt import DbtCliResource
from pipeline.resources.dbt_project import dbt_project  # Tells Dagster where to find our DBT code
from pipeline.constants import LAKE_PATH
import pipeline.defs

# Create resource instances
socrata = SocrataResource()  # Uses default env var for SOCRATA_API_TOKEN
dbt = DbtCliResource(project_dir=dbt_project.project_dir)

single_file_polars_parquet_io_manager = SingleFilePolarsParquetIOManager(base_dir=LAKE_PATH)
fastopendata_partitioned_parquet_io_manager = FastOpenDataPartitionedParquetIOManager(base_dir=LAKE_PATH)

resources = {
    "dbt": dbt,
    "single_file_polars_parquet_io_manager": single_file_polars_parquet_io_manager,
    "fastopendata_partitioned_parquet_io_manager": fastopendata_partitioned_parquet_io_manager,
    "socrata": socrata,
}




# The final, single Definitions object.
defs = dg_components.load_defs(pipeline.defs)
