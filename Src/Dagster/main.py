# from dagster import asset
from dagster import op, job, Out, In, Nothing, String, repository
from ETL import create_fake_data, transform_data, load_data, cleanup_data


# Create Data (Extract)
@op(config_schema={"data_size": String}, out=Out(Nothing))
def create_fake_data_op(context):
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to create fake data of size: {data_size}")
    create_fake_data(data_size)
    context.log.info(f"Finished creating fake data of size: {data_size}")


# Transform Data (Transform JSON to Parquet)
@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def transform_data_op(context):
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to transform data of size: {data_size}")
    transform_data(data_size)
    context.log.info(f"Finished transforming data of size: {data_size}")


# Load Data (Save Parquet)
@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def load_data_op(context):
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to load data of size: {data_size}")
    load_data(data_size)
    context.log.info(f"Finished loading data of size: {data_size}")


# Cleanup Data (Remove Files)
@op(config_schema={"data_size": String}, out=Out(Nothing))
def cleanup_data_op(context):
    cleanup_data(context)


# Small ETL Job
@job
def small_etl_job():
    size = "Small"
    create = create_fake_data_op.configured(
        {"data_size": size}, name=f"{size}create_fake_data"
    )()
    transform = transform_data_op.configured(
        {"data_size": size}, name=f"{size}_transform_data"
    )(start=create)
    load_data_op.configured({"data_size": size}, name=f"{size}_load_data")(
        start=transform
    )


# Medium ETL Job
@job
def medium_etl_job():
    size = "Medium"
    create = create_fake_data_op.configured(
        {"data_size": size}, name=f"{size}create_fake_data"
    )()
    transform = transform_data_op.configured(
        {"data_size": size}, name=f"{size}_transform_data"
    )(start=create)
    load_data_op.configured({"data_size": size}, name=f"{size}_load_data")(
        start=transform
    )


@job
def large_etl_job():
    size = "Large"
    create = create_fake_data_op.configured(
        {"data_size": size}, name=f"{size}create_fake_data"
    )()
    transform = transform_data_op.configured(
        {"data_size": size}, name=f"{size}_transform_data"
    )(start=create)
    load_data_op.configured({"data_size": size}, name=f"{size}_load_data")(
        start=transform
    )


@job
def cleanup_small_etl_job():
    cleanup_data_op.configured({"data_size": "Small"}, name="cleanup_small")()


@job
def cleanup_medium_etl_job():
    cleanup_data_op.configured({"data_size": "Medium"}, name="cleanup_medium")()


@job
def cleanup_large_etl_job():
    cleanup_data_op.configured({"data_size": "Large"}, name="cleanup_large")()


# Repository definition
@repository
def etl_repository():
    return [
        small_etl_job,
        medium_etl_job,
        large_etl_job,
        cleanup_small_etl_job,
        cleanup_medium_etl_job,
        cleanup_large_etl_job,
    ]
