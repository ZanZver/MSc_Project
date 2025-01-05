# from dagster import asset

from dagster import op, job, Out, In, Nothing, String
from ETL import (
    create_fake_data,
    transform_data,
    load_data,
)


# Create Data (Extract)
@op(config_schema={"data_size": String}, out=Out(Nothing))
def create_fake_data_op(context):
    data_size = context.op_config["data_size"]
    create_fake_data(data_size)


# Transform Data (Transform JSON to Parquet)
@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def transform_data_op(context):
    data_size = context.op_config["data_size"]
    transform_data(data_size)


# Load Data (Save Parquet)
@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def load_data_op(context):
    data_size = context.op_config["data_size"]
    load_data(data_size)


# Dagster Job Definition
@job
def small_etl_job():
    create = create_fake_data_op.configured(
        {"data_size": "Small"}, name="create_fake_data_configured"
    )()
    transform = transform_data_op.configured(
        {"data_size": "Small"}, name="transform_data_configured"
    )(start=create)
    load_data_op.configured({"data_size": "Small"}, name="load_data_configured")(
        start=transform
    )
