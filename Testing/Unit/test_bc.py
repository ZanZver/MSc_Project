import sys
import os

# Add the `Project` directory to `sys.path`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import pytest
import json
from unittest.mock import MagicMock, patch
from fastapi import HTTPException
from Src.API.models.models import BlockchainRecord
from API.blockchain import get_latest_record_logic, append_data_logic, delete_record_bc_logic, get_record_history_logic, get_account_logic, get_connection_logic

def test_get_latest_record_logic_success():
    # Mock web3 and blockchain
    mock_w3 = MagicMock()
    mock_w3.eth.block_number = 2

    # Mock blocks and transactions
    block_0 = MagicMock()
    block_0.transactions = []

    block_1 = MagicMock()
    tx_1 = MagicMock()
    tx_1.to = None
    tx_1.input = "0x7b2276696e223a2022313233227d"  # JSON for {"vin": "123"}
    block_1.transactions = [tx_1]

    block_2 = MagicMock()
    block_2.transactions = []

    mock_w3.eth.get_block.side_effect = [block_0, block_1, block_2]
    mock_w3.to_text = lambda hexstr: '{"vin": "123"}'

    # Call the function
    result = get_latest_record_logic(mock_w3, "123")

    # Assertions
    assert result == {"vin": "123"}, "Should return the correct record"

def test_get_latest_record_logic_not_found():
    # Mock web3 and blockchain
    mock_w3 = MagicMock()
    mock_w3.eth.block_number = 1

    # Mock blocks and transactions
    block_0 = MagicMock()
    block_0.transactions = []

    block_1 = MagicMock()
    tx_1 = MagicMock()
    tx_1.to = None
    tx_1.input = "0x7b2276696e223a2022343536227d"  # JSON for {"vin": "456"}
    block_1.transactions = [tx_1]

    mock_w3.eth.get_block.side_effect = [block_0, block_1]
    mock_w3.to_text = lambda hexstr: '{"vin": "456"}'

    # Call the function and expect HTTP 404
    with pytest.raises(HTTPException) as excinfo:
        get_latest_record_logic(mock_w3, "123")
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "Record not found"

def test_get_latest_record_logic_error():
    # Mock web3 to throw an exception
    mock_w3 = MagicMock()
    mock_w3.eth.block_number = 1
    mock_w3.eth.get_block.side_effect = Exception("Blockchain error")

    # Call the function and expect HTTP 500
    with pytest.raises(HTTPException) as excinfo:
        get_latest_record_logic(mock_w3, "123")
    assert excinfo.value.status_code == 500
    assert "Blockchain error" in excinfo.value.detail
    
def test_append_data_logic_success():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    mock_record = MagicMock()
    mock_record.data = {"key": "value"}
    mock_w3.to_hex.return_value = "0x7b2274657374223a202276616c7565227d"  # JSON for {"test": "value"}
    mock_w3.to_wei.return_value = 20000000000  # 20 gwei
    mock_w3.eth.send_transaction.return_value = MagicMock(hex=MagicMock(return_value="0xabcdef"))

    # Call the function
    result = append_data_logic(mock_w3, mock_account, mock_record)

    # Assertions
    assert result == {"transaction_hash": "0xabcdef"}, "Should return the correct transaction hash"
    mock_w3.to_hex.assert_called_once_with(text='{"key": "value"}')
    mock_w3.to_wei.assert_called_once_with("20", "gwei")
    mock_w3.eth.send_transaction.assert_called_once()

def test_append_data_logic_empty_data():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    mock_record = MagicMock()
    mock_record.data = None

    # Call the function and expect HTTP 400
    with pytest.raises(HTTPException) as excinfo:
        append_data_logic(mock_w3, mock_account, mock_record)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Data to append cannot be empty"

def test_append_data_logic_invalid_data_format():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    mock_record = MagicMock()
    mock_record.data = {"key": "value"}
    mock_w3.to_hex.side_effect = ValueError("Invalid data format")

    # Call the function and expect HTTP 400
    with pytest.raises(HTTPException) as excinfo:
        append_data_logic(mock_w3, mock_account, mock_record)
    assert excinfo.value.status_code == 400
    assert "Invalid data format" in excinfo.value.detail

def test_append_data_logic_generic_error():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    mock_record = MagicMock()
    mock_record.data = {"key": "value"}
    mock_w3.eth.send_transaction.side_effect = Exception("Unexpected error")

    # Call the function and expect HTTP 500
    with pytest.raises(HTTPException) as excinfo:
        append_data_logic(mock_w3, mock_account, mock_record)
    assert excinfo.value.status_code == 500
    assert "Unexpected error" in excinfo.value.detail
    

def test_delete_record_bc_logic_success():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    key = "123ABC"
    key_field = "vin"
    deletion_record = {key_field: key, "deleted": True}
    mock_w3.to_hex.return_value = "0x7b2276696e223a2022313233414243222c202264656c65746564223a20747275657d"  # JSON for deletion_record
    mock_w3.to_wei.return_value = 20000000000  # 20 gwei
    mock_w3.eth.send_transaction.return_value = MagicMock(hex=MagicMock(return_value="0xabcdef"))

    # Call the function
    result = delete_record_bc_logic(mock_w3, mock_account, key, key_field)

    # Assertions
    assert result == {"transaction_hash": "0xabcdef"}, "Should return the correct transaction hash"
    mock_w3.to_hex.assert_called_once_with(text=json.dumps(deletion_record))
    mock_w3.to_wei.assert_called_once_with("20", "gwei")
    mock_w3.eth.send_transaction.assert_called_once()

def test_delete_record_bc_logic_empty_key():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    key = ""
    key_field = "vin"

    # Call the function and expect HTTP 400
    with pytest.raises(HTTPException) as excinfo:
        delete_record_bc_logic(mock_w3, mock_account, key, key_field)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Key cannot be empty"

def test_delete_record_bc_logic_invalid_key_field():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    key = "123ABC"
    key_field = "invalid_field"
    mock_w3.to_hex.side_effect = ValueError("Invalid key or key_field")

    # Call the function and expect HTTP 400
    with pytest.raises(HTTPException) as excinfo:
        delete_record_bc_logic(mock_w3, mock_account, key, key_field)
    assert excinfo.value.status_code == 400
    assert "Invalid key or key_field" in excinfo.value.detail

def test_delete_record_bc_logic_generic_error():
    # Mock web3 and account
    mock_w3 = MagicMock()
    mock_account = "0x12345"
    key = "123ABC"
    key_field = "vin"
    mock_w3.eth.send_transaction.side_effect = Exception("Unexpected error")

    # Call the function and expect HTTP 500
    with pytest.raises(HTTPException) as excinfo:
        delete_record_bc_logic(mock_w3, mock_account, key, key_field)
    assert excinfo.value.status_code == 500
    assert "Unexpected error" in excinfo.value.detail

def test_get_record_history_logic_success():
    # Mock web3
    mock_w3 = MagicMock()
    mock_w3.eth.block_number = 2

    # Mock blocks and transactions
    block_0 = MagicMock()
    block_0.transactions = []

    block_1 = MagicMock()
    tx_1 = MagicMock()
    tx_1.to = None
    tx_1.input = "0x7b2276696e223a2022313233227d"  # JSON for {"vin": "123"}
    block_1.transactions = [tx_1]

    block_2 = MagicMock()
    tx_2 = MagicMock()
    tx_2.to = None
    tx_2.input = "0x7b2276696e223a2022313233227d"  # JSON for {"vin": "123"}
    block_2.transactions = [tx_2]

    mock_w3.eth.get_block.side_effect = [block_0, block_1, block_2]
    mock_w3.to_text = lambda hexstr: '{"vin": "123"}'

    # Call the function
    result = get_record_history_logic(mock_w3, "123")

    # Assertions
    assert result == [{"vin": "123"}, {"vin": "123"}], "Should return the correct record history"
    mock_w3.eth.get_block.assert_called()

def test_get_record_history_logic_no_history():
    # Mock web3
    mock_w3 = MagicMock()
    mock_w3.eth.block_number = 1

    # Mock blocks and transactions
    block_0 = MagicMock()
    block_0.transactions = []

    block_1 = MagicMock()
    tx_1 = MagicMock()
    tx_1.to = None
    tx_1.input = "0x7b2276696e223a2022343536227d"  # JSON for {"vin": "456"}
    block_1.transactions = [tx_1]

    mock_w3.eth.get_block.side_effect = [block_0, block_1]
    mock_w3.to_text = lambda hexstr: '{"vin": "456"}'

    # Call the function and expect HTTP 404
    with pytest.raises(HTTPException) as excinfo:
        get_record_history_logic(mock_w3, "123")
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "No history found for the record"

def test_get_record_history_logic_error():
    # Mock web3 to throw an exception
    mock_w3 = MagicMock()
    mock_w3.eth.block_number = 1
    mock_w3.eth.get_block.side_effect = Exception("Blockchain error")

    # Call the function and expect HTTP 500
    with pytest.raises(HTTPException) as excinfo:
        get_record_history_logic(mock_w3, "123")
    assert excinfo.value.status_code == 500
    assert "Blockchain error" in excinfo.value.detail


def test_test_account_logic_valid_account():
    # Test with a valid account
    account = "0x12345"
    result = get_account_logic(account)
    assert result == {"account": account}, "Should return the correct account dictionary"

def test_test_account_logic_no_account():
    # Test with no account
    account = None
    with pytest.raises(HTTPException) as excinfo:
        get_account_logic(account)
    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == "Account not initialized."

def test_get_connection_logic_connected():
    # Mock web3 connection
    mock_w3 = MagicMock()
    mock_w3.is_connected.return_value = True

    # Call the function
    result = get_connection_logic(mock_w3)

    # Assertions
    assert result == {"message": "Connected to blockchain"}, "Should return success message"
    mock_w3.is_connected.assert_called_once()

def test_get_connection_logic_not_connected():
    # Mock web3 connection
    mock_w3 = MagicMock()
    mock_w3.is_connected.return_value = False

    # Call the function and expect HTTP 500
    with pytest.raises(HTTPException) as excinfo:
        get_connection_logic(mock_w3)
    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == "Blockchain connection is not active."
    mock_w3.is_connected.assert_called_once()


