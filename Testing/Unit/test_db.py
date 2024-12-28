# test_get_all_data_logic.py
import pytest
from unittest.mock import MagicMock
from fastapi import HTTPException
#from API.db import get_all_data_logic
from API.db import update_record_logic, get_all_data_logic, get_specific_data_logic, delete_record_db_logic
import json

def test_get_all_data_logic_success():
    # Data to return from fetchall
    mock_data = [{"id": 1, "name": "Vehicle1"}, {"id": 2, "name": "Vehicle2"}]

    # Mock cursor
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_data

    # Mock connection to return the mock cursor
    mock_connection = MagicMock()
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock get_db_connection to return the mock connection
    mock_get_db_connection = MagicMock(return_value=mock_connection)

    # Call the function
    result = get_all_data_logic(mock_get_db_connection)

    # Assertions
    assert result == mock_data
    mock_cursor.execute.assert_called_once_with("SELECT * FROM vehicles;")

def test_get_all_data_logic_failure():
    # Mock get_db_connection to raise an exception
    mock_get_db_connection = MagicMock(side_effect=Exception("Database connection error"))

    # Call the function and assert exception is raised
    with pytest.raises(HTTPException) as exc_info:
        get_all_data_logic(mock_get_db_connection)

    # Assertions
    assert exc_info.value.status_code == 500
    assert "Error retrieving data" in exc_info.value.detail

def test_get_specific_data_logic_success():
    # Mock data to be returned
    mock_data = [{"id": 1, "vin": "VIN123", "name": "Vehicle1"}]

    # Mock cursor
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_data

    # Mock connection
    mock_connection = MagicMock()
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock get_db_connection
    mock_get_db_connection = MagicMock(return_value=mock_connection)

    # Call the function
    key = "VIN123"
    key_field = "vin"
    result = get_specific_data_logic(mock_get_db_connection, key, key_field)

    # Assertions
    assert result == mock_data
    mock_cursor.execute.assert_called_once_with("SELECT * FROM vehicles WHERE vin = 'VIN123'", ())

def test_get_specific_data_logic_with_params():
    # Mock data to be returned
    mock_data = [{"id": 1, "vin": "VIN123", "name": "Vehicle1"}]

    # Mock cursor
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_data

    # Mock connection
    mock_connection = MagicMock()
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock get_db_connection
    mock_get_db_connection = MagicMock(return_value=mock_connection)

    # Call the function
    key = "VIN123"
    key_field = "vin"
    params = ["param1", "param2"]
    result = get_specific_data_logic(mock_get_db_connection, key, key_field, params)

    # Assertions
    assert result == mock_data
    mock_cursor.execute.assert_called_once_with("SELECT * FROM vehicles WHERE vin = 'VIN123'", params)

def test_get_specific_data_logic_failure():
    # Mock get_db_connection to raise an exception
    mock_get_db_connection = MagicMock(side_effect=Exception("Database error"))

    # Call the function and assert exception is raised
    key = "VIN123"
    key_field = "vin"
    with pytest.raises(HTTPException) as exc_info:
        get_specific_data_logic(mock_get_db_connection, key, key_field)

    # Assertions
    assert exc_info.value.status_code == 500
    assert "Error retrieving data" in exc_info.value.detail

def test_update_record_logic_success():
    # Mock data for update
    update_values = json.dumps({"name": "Updated Vehicle", "status": "Active"})
    key = "VIN123"
    key_field = "vin"

    # Mock cursor
    mock_cursor = MagicMock()

    # Mock connection
    mock_connection = MagicMock()
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock get_db_connection
    mock_get_db_connection = MagicMock(return_value=mock_connection)

    # Call the function
    result = update_record_logic(mock_get_db_connection, update_values, key, key_field)

    # Assertions
    expected_query = "UPDATE vehicles SET name = %s, status = %s WHERE vin = 'VIN123'"
    mock_cursor.execute.assert_called_once_with(expected_query, ("Updated Vehicle", "Active"))
    assert result == {"message": "Record updated successfully"}

def test_update_record_logic_failure():
    # Mock get_db_connection to raise an exception
    mock_get_db_connection = MagicMock(side_effect=Exception("Database error"))

    # Mock data for update
    update_values = json.dumps({"name": "Updated Vehicle", "status": "Active"})
    key = "VIN123"
    key_field = "vin"

    # Call the function and assert exception is raised
    with pytest.raises(HTTPException) as exc_info:
        update_record_logic(mock_get_db_connection, update_values, key, key_field)

    # Assertions
    assert exc_info.value.status_code == 500
    assert "Error updating record" in exc_info.value.detail

def test_delete_record_db_logic_success():
    # Mock data for deletion
    key = "VIN123"
    key_field = "vin"

    # Mock cursor
    mock_cursor = MagicMock()

    # Mock connection
    mock_connection = MagicMock()
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock get_db_connection
    mock_get_db_connection = MagicMock(return_value=mock_connection)

    # Call the function
    result = delete_record_db_logic(mock_get_db_connection, key, key_field)

    # Assertions
    expected_query = "DELETE FROM vehicles WHERE vin = 'VIN123'"
    mock_cursor.execute.assert_called_once_with(expected_query)
    assert result == {"message": "Record deleted successfully"}

def test_delete_record_db_logic_failure():
    # Mock get_db_connection to raise an exception
    mock_get_db_connection = MagicMock(side_effect=Exception("Database error"))

    # Mock data for deletion
    key = "VIN123"
    key_field = "vin"

    # Call the function and assert exception is raised
    with pytest.raises(HTTPException) as exc_info:
        delete_record_db_logic(mock_get_db_connection, key, key_field)

    # Assertions
    assert exc_info.value.status_code == 500
    assert "Error deleting record" in exc_info.value.detail




