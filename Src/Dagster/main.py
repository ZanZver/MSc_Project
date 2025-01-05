# from dagster import asset

from dagster import op, job, Out, In, Nothing
from ETL import (
    create_fake_data,
    transform_data,
    load_data,
)


# Create Data (Extract)
@op(out=Out(Nothing))  # No return value; signals the next step when done
def create_fake_data_op():
    create_fake_data("Small")


# Transform Data (Transform JSON to Parquet)
@op(ins={"start": In(Nothing)}, out=Out(Nothing))  # Depends on create_fake_data_op
def transform_data_op():
    transform_data("Small")


# Load Data (Save Parquet)
@op(ins={"start": In(Nothing)}, out=Out(Nothing))  # Depends on transform_data_op
def load_data_op():
    load_data("Small")


# Dagster Job Definition
@job
def small_etl_job():
    # Set execution dependencies using explicit wiring
    create = create_fake_data_op()
    transform = transform_data_op(start=create)
    load_data_op(start=transform)
