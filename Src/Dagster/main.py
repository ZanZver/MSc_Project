# from dagster import asset
from dagster import op, job, Out, In, Nothing, String, repository
from ETL import (
    create_fake_data,
    transform_data,
    load_data,
    cleanup_data,
    bc_insert_data,
    db_insert_data,
)
import os


# Create Data (Extract)
@op(config_schema={"data_size": String}, out=Out(Nothing))
def create_fake_data_op(context: dict) -> None:
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to create fake data of size: {data_size}")
    create_fake_data(data_size)
    context.log.info(f"Finished creating fake data of size: {data_size}")


# Transform Data (Transform JSON to Parquet)
@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def transform_data_op(context: dict) -> None:
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to transform data of size: {data_size}")
    transform_data(data_size)
    context.log.info(f"Finished transforming data of size: {data_size}")


# Load Data (Save Parquet)
@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def load_data_op(context: dict) -> None:
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to load data of size: {data_size}")
    load_data(data_size)
    context.log.info(f"Finished loading data of size: {data_size}")


@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def bc_insert_data_op(context: dict) -> None:
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to insert blockchain data of size: {data_size}")
    bc_insert_data(data_size)
    context.log.info(f"Finished inserting blockchain data of size: {data_size}")


@op(config_schema={"data_size": String}, ins={"start": In(Nothing)}, out=Out(Nothing))
def db_insert_data_op(context: dict) -> None:
    data_size = context.op_config["data_size"]
    context.log.info(f"Starting to insert db data of size: {data_size}")
    db_insert_data(data_size)
    context.log.info(f"Finished inserting db data of size: {data_size}")


# Cleanup Data (Remove Files)
@op(config_schema={"data_size": String}, out=Out(Nothing))
def cleanup_data_op(context: dict) -> None:
    cleanup_data(context)


@op(config_schema={"container": String}, out=Out(Nothing))
def start_docker_compose_op(context: dict) -> None:
    container = context.op_config["container"]
    os.system(f"docker-compose -f ../Docker/{container}/docker-compose.yml up -d")


# Small ETL Job
@job
def small_etl_job() -> None:
    size = "Small"
    create = create_fake_data_op.configured(
        {"data_size": size}, name=f"{size}create_fake_data"
    )()
    transform = transform_data_op.configured(
        {"data_size": size}, name=f"{size}_transform_data"
    )(start=create)
    load = load_data_op.configured({"data_size": size}, name=f"{size}_load_data")(
        start=transform
    )
    bc_insert_data_op.configured({"data_size": size}, name=f"{size}_bc_insert_data")(
        start=load
    )
    db_insert_data_op.configured({"data_size": size}, name=f"{size}_db_insert_data")(
        start=load
    )


# Medium ETL Job
@job
def medium_etl_job() -> None:
    size = "Medium"
    create = create_fake_data_op.configured(
        {"data_size": size}, name=f"{size}create_fake_data"
    )()
    transform = transform_data_op.configured(
        {"data_size": size}, name=f"{size}_transform_data"
    )(start=create)
    load = load_data_op.configured({"data_size": size}, name=f"{size}_load_data")(
        start=transform
    )
    bc_insert_data_op.configured({"data_size": size}, name=f"{size}_bc_insert_data")(
        start=load
    )
    db_insert_data_op.configured({"data_size": size}, name=f"{size}_db_insert_data")(
        start=load
    )


@job
def large_etl_job() -> None:
    size = "Large"
    create = create_fake_data_op.configured(
        {"data_size": size}, name=f"{size}create_fake_data"
    )()
    transform = transform_data_op.configured(
        {"data_size": size}, name=f"{size}_transform_data"
    )(start=create)
    load = load_data_op.configured({"data_size": size}, name=f"{size}_load_data")(
        start=transform
    )
    bc_insert_data_op.configured({"data_size": size}, name=f"{size}_bc_insert_data")(
        start=load
    )
    db_insert_data_op.configured({"data_size": size}, name=f"{size}_db_insert_data")(
        start=load
    )


@job
def cleanup_small_etl_job() -> None:
    cleanup_data_op.configured({"data_size": "Small"}, name="cleanup_small")()


@job
def cleanup_medium_etl_job() -> None:
    cleanup_data_op.configured({"data_size": "Medium"}, name="cleanup_medium")()


@job
def cleanup_large_etl_job() -> None:
    cleanup_data_op.configured({"data_size": "Large"}, name="cleanup_large")()


@job
def start_ethereum_docker() -> None:
    start_docker_compose_op.configured(
        {"container": "ethereum"}, name="ethereum_docker"
    )()


@job
def start_db_docker() -> None:
    start_docker_compose_op.configured({"container": "db"}, name="db_docker")()


# Repository definition
@repository
def etl_repository() -> list:
    return [
        small_etl_job,
        medium_etl_job,
        large_etl_job,
        cleanup_small_etl_job,
        cleanup_medium_etl_job,
        cleanup_large_etl_job,
        start_ethereum_docker,
        start_db_docker,
    ]
