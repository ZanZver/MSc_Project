import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_op_context
from ETL import cleanup_data
from ETL import transform_data
from ETL import create_fake_data
from ETL import load_data
from ETL import bc_insert_data
from ETL.insert_db import create_table_from_df, db_insert_data


# Mocking any external dependencies if necessary
# For example, if create_fake_data uses an external API, mock it here
# @patch('ETL.external_api_call')
@pytest.mark.parametrize(
    "size, expected_num",
    [
        ("Small", 1000),
        ("Medium", 5000),
        ("Large", 10000),
        ("SomethingElse", 1000),
    ],
)
def test_create_fake_data_file_creation(size, expected_num):
    """
    Test create_fake_data to ensure it creates a JSON file with the correct structure and size.
    """
    mock_dump = MagicMock()

    # Mock json.dump to avoid file writes
    with patch("json.dump", mock_dump), patch("os.makedirs", return_value=True):
        create_fake_data(size)

    # Extract the written data passed to json.dump
    written_data = mock_dump.call_args[0][0]

    # Assertions
    assert isinstance(written_data, list), f"Expected list, got {type(written_data)}"
    assert (
        len(written_data) == expected_num
    ), f"Expected {expected_num} entries, got {len(written_data)}"

    # Check structure of the first element
    first_item = written_data[0]
    expected_keys = [
        "vin",
        "license_plate",
        "vehicle_make",
        "vehicle_model",
        "vehicle_year",
        "full_vehicleInfo",
        "vehicle_category",
        "vehicle_make_model",
        "vehicle_year_make_model",
        "vehicle_year_make_model_cat",
    ]
    assert all(
        key in first_item for key in expected_keys
    ), "Missing keys in the vehicle data dictionary"


@pytest.mark.parametrize("size", ["small", "medium", "large"])
def test_transform_data(size):
    """
    Test transform_data to ensure JSON is read and Parquet is written correctly.
    """
    mock_df = pl.DataFrame(
        {"vin": ["1234ABC"], "vehicle_make": ["Toyota"], "vehicle_year": [2021]}
    )

    with patch("polars.read_json", return_value=mock_df) as mock_read_json, patch(
        "polars.DataFrame.write_parquet"
    ) as mock_write_parquet, patch("os.makedirs", return_value=True):
        transform_data(size)

    # Assertions
    input_json_path = f"../Data/Extract/{size}/data.json"
    output_parquet_path = f"../Data/Transform/{size}/data.parquet"
    mock_read_json.assert_called_once_with(input_json_path)
    mock_write_parquet.assert_called_once_with(output_parquet_path)


@pytest.mark.parametrize("size", ["small", "medium", "large"])
def test_load_data(size):
    """
    Test load_data to ensure the Parquet file is read and written correctly.
    """
    mock_df = pl.DataFrame(
        {"vin": ["1234XYZ"], "vehicle_make": ["Honda"], "vehicle_year": [2022]}
    )

    with patch("polars.read_parquet", return_value=mock_df) as mock_read_parquet, patch(
        "polars.DataFrame.write_parquet"
    ) as mock_write_parquet, patch("os.makedirs", return_value=True):
        load_data(size)

    # Assertions
    input_parquet_path = f"../Data/Transform/{size}/data.parquet"
    output_parquet_path = f"../Data/Load/{size}/data.parquet"
    mock_read_parquet.assert_called_once_with(input_parquet_path)
    mock_write_parquet.assert_called_once_with(output_parquet_path)


@pytest.mark.parametrize(
    "data_size, existing_files",
    [
        ("small", [True, True, True]),  # All files exist
        ("medium", [True, False, True]),  # One file is missing
        ("large", [False, False, False]),  # No files exist
    ],
)
def test_cleanup_data(data_size, existing_files):
    """
    Test cleanup_data to ensure files are removed correctly and appropriate log messages are generated.
    """
    context = build_op_context(config={"data_size": data_size})

    paths = [
        f"../Data/Extract/{data_size}/data.json",
        f"../Data/Transform/{data_size}/data.parquet",
        f"../Data/Load/{data_size}/data.parquet",
    ]

    with patch("os.path.exists", side_effect=existing_files) as _, patch(
        "os.remove"
    ) as mock_remove, patch.object(context.log, "info") as mock_info, patch.object(
        context.log, "warning"
    ) as mock_warning, patch.object(
        context.log, "error"
    ) as _:
        cleanup_data(context)

    # Assertions
    for i, path in enumerate(paths):
        if existing_files[i]:
            mock_remove.assert_any_call(path)
            mock_info.assert_any_call(f"Successfully removed file: {path}")
        else:
            mock_warning.assert_any_call(f"File not found (nothing to remove): {path}")

    # Ensure no unexpected calls
    assert mock_remove.call_count == sum(
        existing_files
    ), "Unexpected number of remove calls"


@pytest.fixture
def sample_data():
    return [
        {"vin": "1234ABC", "vehicle_make": "Toyota", "vehicle_year": 2021},
        {"vin": "5678DEF", "vehicle_make": "Honda", "vehicle_year": 2020},
    ]


@pytest.fixture
def mock_connection():
    mock_w3 = MagicMock()
    mock_w3.eth.accounts = ["0xAccount123"]
    mock_w3.is_connected.return_value = True  # Simulate successful connection
    mock_w3.eth.send_transaction.return_value = "0xTransactionHash"
    return "0xAccount123", mock_w3


@patch("ETL.insert_bc.create_connection")
@patch("polars.read_parquet")
def test_bc_insert_data(
    mock_read_parquet, mock_create_connection, sample_data, mock_connection
):
    """
    Test bc_insert_data to ensure data is read from Parquet and inserted into the blockchain.
    """
    account, mock_w3 = mock_connection
    mock_create_connection.return_value = (account, mock_w3)
    mock_read_parquet.return_value = pl.DataFrame(sample_data)

    # Run the function
    bc_insert_data("small")

    # Assertions
    mock_read_parquet.assert_called_once_with("../Data/Transform/small/data.parquet")
    assert mock_w3.eth.send_transaction.call_count == len(
        sample_data
    ), "Unexpected number of transactions sent"


@pytest.fixture
def sample_records():
    return [
        {
            "vin": "1234ABC",
            "vehicle_make": "Toyota",
            "vehicle_year": 2021,
            "Make": "Toyota",
            "Model": "Corolla",
            "Year": 2021,
            "Category": "Sedan",
        },
        {
            "vin": "5678DEF",
            "vehicle_make": "Honda",
            "vehicle_year": 2020,
            "Make": "Honda",
            "Model": "Civic",
            "Year": 2020,
            "Category": "Coupe",
        },
    ]


@pytest.fixture
def sample_df(sample_records):
    return pl.DataFrame(sample_records)


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
def test_db_insert_data(
    mock_connect, mock_insert_data, mock_create_table, mock_load_data, sample_df
):
    """
    Test db_insert_data to ensure the full pipeline is executed correctly.
    """
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_load_data.return_value = sample_df

    db_insert_data("small")

    # Assertions
    mock_load_data.assert_called_once_with("../Data/Transform/small/data.parquet")
    mock_create_table.assert_called_once()
    mock_insert_data.assert_called_once()
