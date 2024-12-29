import pytest
from unittest.mock import MagicMock
from fastapi import HTTPException
from db import (
    update_record_logic,
    get_all_data_logic,
    get_specific_data_logic,
    delete_record_db_logic,
)
import json


# Common fixtures
@pytest.fixture
def mock_cursor():
    cursor = MagicMock()
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = mock_cursor
    return connection


@pytest.fixture
def mock_get_db_connection(mock_connection):
    return MagicMock(return_value=mock_connection)


# Tests
def test_get_all_data_logic_success(mock_get_db_connection, mock_cursor):
    mock_data = [{"id": 1, "name": "Vehicle1"}, {"id": 2, "name": "Vehicle2"}]
    mock_cursor.fetchall.return_value = mock_data

    result = get_all_data_logic(mock_get_db_connection)

    assert result == mock_data
    mock_cursor.execute.assert_called_once_with("SELECT * FROM vehicles;")


def test_get_all_data_logic_failure(mock_get_db_connection):
    mock_get_db_connection.side_effect = Exception("Database connection error")

    with pytest.raises(HTTPException) as exc_info:
        get_all_data_logic(mock_get_db_connection)

    assert exc_info.value.status_code == 500
    assert "Error retrieving data" in exc_info.value.detail


def test_get_specific_data_logic_success(mock_get_db_connection, mock_cursor):
    mock_data = [{"id": 1, "vin": "VIN123", "name": "Vehicle1"}]
    mock_cursor.fetchall.return_value = mock_data

    result = get_specific_data_logic(mock_get_db_connection, "VIN123", "vin")

    assert result == mock_data
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM vehicles WHERE vin = 'VIN123'", ()
    )


def test_get_specific_data_logic_failure(mock_get_db_connection):
    mock_get_db_connection.side_effect = Exception("Database error")

    with pytest.raises(HTTPException) as exc_info:
        get_specific_data_logic(mock_get_db_connection, "VIN123", "vin")

    assert exc_info.value.status_code == 500
    assert "Error retrieving data" in exc_info.value.detail


def test_update_record_logic_success(mock_get_db_connection, mock_cursor):
    update_values = json.dumps({"name": "Updated Vehicle", "status": "Active"})

    result = update_record_logic(mock_get_db_connection, update_values, "VIN123", "vin")

    expected_query = "UPDATE vehicles SET name = %s, status = %s WHERE vin = 'VIN123'"
    mock_cursor.execute.assert_called_once_with(
        expected_query, ("Updated Vehicle", "Active")
    )
    assert result == {"message": "Record updated successfully"}


def test_update_record_logic_failure(mock_get_db_connection):
    mock_get_db_connection.side_effect = Exception("Database error")

    update_values = json.dumps({"name": "Updated Vehicle", "status": "Active"})

    with pytest.raises(HTTPException) as exc_info:
        update_record_logic(mock_get_db_connection, update_values, "VIN123", "vin")

    assert exc_info.value.status_code == 500
    assert "Error updating record" in exc_info.value.detail


def test_delete_record_db_logic_success(mock_get_db_connection, mock_cursor):
    result = delete_record_db_logic(mock_get_db_connection, "VIN123", "vin")

    expected_query = "DELETE FROM vehicles WHERE vin = 'VIN123'"
    mock_cursor.execute.assert_called_once_with(expected_query)
    assert result == {"message": "Record deleted successfully"}


def test_delete_record_db_logic_failure(mock_get_db_connection):
    mock_get_db_connection.side_effect = Exception("Database error")

    with pytest.raises(HTTPException) as exc_info:
        delete_record_db_logic(mock_get_db_connection, "VIN123", "vin")

    assert exc_info.value.status_code == 500
    assert "Error deleting record" in exc_info.value.detail
