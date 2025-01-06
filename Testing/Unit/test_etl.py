import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_op_context
from ETL import create_fake_data, transform_data, load_data, cleanup_data
from ETL.insert_db import create_table_from_df, db_insert_data
from ETL import bc_insert_data


# Fixtures for shared data
@pytest.fixture
def sample_records():
    return [
        {"vin": "1234ABC", "vehicle_make": "Toyota", "vehicle_year": 2021},
        {"vin": "5678DEF", "vehicle_make": "Honda", "vehicle_year": 2020},
    ]


@pytest.fixture
def sample_df(sample_records):
    return pl.DataFrame(sample_records)


@pytest.fixture
def mock_connection():
    mock_w3 = MagicMock()
    mock_w3.eth.accounts = ["0xAccount123"]
    mock_w3.is_connected.return_value = True
    mock_w3.eth.send_transaction.return_value = "0xTransactionHash"
    return "0xAccount123", mock_w3


# Test create_fake_data
@pytest.mark.parametrize(
    "size, expected_num", [("Small", 1000), ("Medium", 5000), ("Large", 10000)]
)
def test_create_fake_data(size, expected_num):
    with patch("json.dump") as mock_dump, patch("os.makedirs", return_value=True):
        create_fake_data(size)
    assert len(mock_dump.call_args[0][0]) == expected_num


# Test transform_data
@pytest.mark.parametrize("size", ["Small", "Medium", "Large"])
def test_transform_data(size):
    mock_df = pl.DataFrame(
        {"vin": ["1234ABC"], "vehicle_make": ["Toyota"], "vehicle_year": [2021]}
    )
    with patch("polars.read_json", return_value=mock_df) as mock_read, patch(
        "polars.DataFrame.write_parquet"
    ) as mock_write:
        transform_data(size)
    mock_read.assert_called_once_with(f"../Data/Extract/{size}/data.json")
    mock_write.assert_called_once_with(f"../Data/Transform/{size}/data.parquet")


# Test load_data
@pytest.mark.parametrize("size", ["Small", "Medium", "Large"])
def test_load_data(size):
    mock_df = pl.DataFrame(
        {"vin": ["1234XYZ"], "vehicle_make": ["Honda"], "vehicle_year": [2022]}
    )
    with patch("polars.read_parquet", return_value=mock_df) as mock_read, patch(
        "polars.DataFrame.write_parquet"
    ) as mock_write:
        load_data(size)
    mock_read.assert_called_once_with(f"../Data/Transform/{size}/data.parquet")
    mock_write.assert_called_once_with(f"../Data/Load/{size}/data.parquet")


# Test cleanup_data
@pytest.mark.parametrize(
    "data_size, file_existence",
    [
        ("Small", [True, True, True]),
        ("Medium", [True, False, True]),
        ("Large", [False, False, False]),
    ],
)
def test_cleanup_data(data_size, file_existence):
    context = build_op_context(config={"data_size": data_size})
    # paths = [
    #     f"../Data/{folder}/{data_size}/data.{ext}"
    #     for folder, ext in [
    #         ("Extract", "json"),
    #         ("Transform", "parquet"),
    #         ("Load", "parquet"),
    #     ]
    # ]
    with patch("os.path.exists", side_effect=file_existence) as _, patch(
        "os.remove"
    ) as mock_remove:
        cleanup_data(context)
    assert mock_remove.call_count == sum(file_existence)


# Test blockchain insert
@patch("ETL.insert_bc.create_connection")
@patch("polars.read_parquet")
@pytest.mark.parametrize("size", ["Small", "Medium", "Large"])
def test_bc_insert_data(
    mock_read_parquet, mock_create_connection, sample_records, mock_connection, size
):
    """
    Test bc_insert_data to ensure data is read from Parquet and inserted into the blockchain.
    """
    account, mock_w3 = mock_connection
    mock_create_connection.return_value = (account, mock_w3)
    mock_read_parquet.return_value = pl.DataFrame(sample_records)

    # Run the function
    bc_insert_data(size)

    # Assertions
    mock_read_parquet.assert_called_once_with(f"../Data/Transform/{size}/data.parquet")
    assert mock_w3.eth.send_transaction.call_count == len(
        sample_records
    ), "Unexpected number of transactions sent"


# Test PostgreSQL insert
@patch("psycopg2.connect")
def test_create_table_from_df(mock_connect, sample_df):
    """
    Test create_table_from_df to ensure the PostgreSQL table creation query is executed correctly.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = (
        mock_cursor  # Ensure the context manager returns the correct cursor
    )
    mock_connect.return_value = mock_conn

    create_table_from_df("test_table", sample_df)

    # Assertions
    assert mock_connect.called, "Database connection not established"
    assert mock_cursor.execute.called, "Table creation query not executed"
    create_query = mock_cursor.execute.call_args[0][0]
    print(f"Executed Query: {create_query}")
    assert (
        "CREATE TABLE IF NOT EXISTS test_table" in create_query
    ), "Invalid CREATE TABLE statement"
    assert "vin TEXT" in create_query, "VIN column missing or incorrect type"


@patch("ETL.insert_db.load_and_prepare_data")
@patch("ETL.insert_db.create_table_from_df")
@patch("ETL.insert_db.insert_data_into_db")
@patch("psycopg2.connect")
@pytest.mark.parametrize("size", ["Small", "Medium", "Large"])
def test_db_insert_data(
    mock_connect, mock_insert, mock_create_table, mock_load_data, sample_df, size
):
    mock_connect.return_value = MagicMock()
    mock_load_data.return_value = sample_df
    db_insert_data(size)
    mock_load_data.assert_called_once()
    mock_create_table.assert_called_once()
    mock_insert.assert_called_once()
