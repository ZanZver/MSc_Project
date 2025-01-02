import pytest
import json
from unittest.mock import MagicMock
from fastapi import HTTPException
from blockchain import (
    get_latest_record_logic,
    get_all_records_logic,
    append_data_logic,
    delete_record_bc_logic,
    get_record_history_logic,
    get_account_logic,
    get_connection_logic,
)


# Pytest fixtures
@pytest.fixture
def mock_web3():
    return MagicMock()


@pytest.fixture
def mock_account():
    return "0x12345"


@pytest.fixture
def mock_record():
    record = MagicMock()
    record.data = {"key": "value"}
    return record


# Utility function to mock blockchain blocks
def mock_blocks(mock_w3, blocks):
    mock_w3.eth.block_number = len(blocks) - 1
    mock_w3.eth.get_block.side_effect = blocks


# Test cases
def test_get_latest_record_logic_success(mock_web3):
    block_0 = MagicMock(transactions=[])
    block_1 = MagicMock(
        transactions=[MagicMock(to=None, input="0x7b2276696e223a2022313233227d")]
    )
    block_2 = MagicMock(transactions=[])

    mock_blocks(mock_web3, [block_0, block_1, block_2])
    mock_web3.to_text = lambda hexstr: '{"vin": "123"}'

    result = get_latest_record_logic(mock_web3, "123")
    assert result == {"vin": "123"}, "Should return the correct record"


def test_get_latest_record_logic_not_found(mock_web3):
    block_0 = MagicMock(transactions=[])
    block_1 = MagicMock(
        transactions=[MagicMock(to=None, input="0x7b2276696e223a2022343536227d")]
    )

    mock_blocks(mock_web3, [block_0, block_1])
    mock_web3.to_text = lambda hexstr: '{"vin": "456"}'

    with pytest.raises(HTTPException) as excinfo:
        get_latest_record_logic(mock_web3, "123")
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "Record not found"


def test_get_all_records_logic_success(mock_web3):
    # Mock the blockchain structure
    block_0 = MagicMock(transactions=[])
    block_1 = MagicMock(
        transactions=[
            MagicMock(
                to=None, input=MagicMock(hex=lambda: "7b2276696e223a2022313233227d")
            )  # {"vin": "123"}
        ]
    )
    block_2 = MagicMock(
        transactions=[
            MagicMock(
                to=None, input=MagicMock(hex=lambda: "7b2276696e223a2022343536227d")
            )  # {"vin": "456"}
        ]
    )

    mock_web3.eth.block_number = 2
    mock_web3.eth.get_block.side_effect = [block_0, block_1, block_2]
    mock_web3.to_text.side_effect = (
        lambda hexstr: '{"vin": "123"}'
        if hexstr == "7b2276696e223a2022313233227d"
        else '{"vin": "456"}'
    )

    result = get_all_records_logic(mock_web3)

    assert result == [{"vin": "123"}, {"vin": "456"}], "Should return all valid records"


def test_get_all_records_logic_invalid_data(mock_web3):
    # Mock blockchain with one valid and one invalid transaction
    block_0 = MagicMock(transactions=[])
    block_1 = MagicMock(
        transactions=[
            MagicMock(
                to=None, input=MagicMock(hex=lambda: "7b2276696e223a2022313233227d")
            ),  # {"vin": "123"}
            MagicMock(
                to=None, input=MagicMock(hex=lambda: "invalidhex")
            ),  # Invalid hex input
        ]
    )

    mock_web3.eth.block_number = 1
    mock_web3.eth.get_block.side_effect = [block_0, block_1]
    mock_web3.to_text.side_effect = (
        lambda hexstr: '{"vin": "123"}'
        if hexstr == "7b2276696e223a2022313233227d"
        else None
    )

    result = get_all_records_logic(mock_web3)

    assert result == [
        {"vin": "123"}
    ], "Should skip invalid transactions and return valid ones"


def test_get_all_records_logic_blockchain_error(mock_web3):
    # Simulate a blockchain error
    mock_web3.eth.block_number = 1
    mock_web3.eth.get_block.side_effect = Exception("Blockchain access error")

    with pytest.raises(HTTPException) as excinfo:
        get_all_records_logic(mock_web3)

    assert excinfo.value.status_code == 500
    assert "Error retrieving record: Blockchain access error" in excinfo.value.detail


def test_get_all_records_logic_empty_blockchain(mock_web3):
    # Simulate an empty blockchain
    mock_web3.eth.block_number = -1

    result = get_all_records_logic(mock_web3)

    assert result == [], "Should return an empty list when there are no blocks"


def test_append_data_logic_success(mock_web3, mock_account, mock_record):
    mock_web3.to_hex.return_value = "0x7b2274657374223a202276616c7565227d"
    mock_web3.to_wei.return_value = 20000000000
    mock_web3.eth.send_transaction.return_value = MagicMock(
        hex=MagicMock(return_value="0xabcdef")
    )

    result = append_data_logic(mock_web3, mock_account, mock_record)
    assert result == {
        "transaction_hash": "0xabcdef"
    }, "Should return the correct transaction hash"


def test_append_data_logic_empty_data(mock_web3, mock_account):
    mock_record = MagicMock(data=None)

    with pytest.raises(HTTPException) as excinfo:
        append_data_logic(mock_web3, mock_account, mock_record)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Data to append cannot be empty"


def test_delete_record_bc_logic_success(mock_web3, mock_account):
    key = "123ABC"
    key_field = "vin"
    deletion_record = {key_field: key, "deleted": True}

    mock_web3.to_hex.return_value = json.dumps(deletion_record)
    mock_web3.to_wei.return_value = 20000000000
    mock_web3.eth.send_transaction.return_value = MagicMock(
        hex=MagicMock(return_value="0xabcdef")
    )

    result = delete_record_bc_logic(mock_web3, mock_account, key, key_field)
    assert result == {
        "transaction_hash": "0xabcdef"
    }, "Should return the correct transaction hash"


def test_get_record_history_logic_success(mock_web3):
    block_0 = MagicMock(transactions=[])
    block_1 = MagicMock(
        transactions=[MagicMock(to=None, input="0x7b2276696e223a2022313233227d")]
    )
    block_2 = MagicMock(
        transactions=[MagicMock(to=None, input="0x7b2276696e223a2022313233227d")]
    )

    mock_blocks(mock_web3, [block_0, block_1, block_2])
    mock_web3.to_text = lambda hexstr: '{"vin": "123"}'

    result = get_record_history_logic(mock_web3, "123")
    assert result == [
        {"vin": "123"},
        {"vin": "123"},
    ], "Should return the correct record history"


def test_get_record_history_logic_no_history(mock_web3):
    block_0 = MagicMock(transactions=[])
    block_1 = MagicMock(
        transactions=[MagicMock(to=None, input="0x7b2276696e223a2022343536227d")]
    )

    mock_blocks(mock_web3, [block_0, block_1])
    mock_web3.to_text = lambda hexstr: '{"vin": "456"}'

    with pytest.raises(HTTPException) as excinfo:
        get_record_history_logic(mock_web3, "123")
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "No history found for the record"


def test_get_record_history_logic_error(mock_web3):
    mock_web3.eth.block_number = 1
    mock_web3.eth.get_block.side_effect = Exception("Blockchain error")

    with pytest.raises(HTTPException) as excinfo:
        get_record_history_logic(mock_web3, "123")
    assert excinfo.value.status_code == 500
    assert "Blockchain error" in excinfo.value.detail


def test_get_connection_logic_connected(mock_web3):
    mock_web3.is_connected.return_value = True

    result = get_connection_logic(mock_web3)
    assert result == {
        "message": "Connected to blockchain"
    }, "Should return success message"


def test_get_bc_connection_logic_not_connected(mock_web3):
    mock_web3.is_connected.return_value = False

    with pytest.raises(HTTPException) as excinfo:
        get_connection_logic(mock_web3)
    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == "Blockchain connection is not active."


def test_account_logic_valid_account():
    # Test with a valid account
    account = "0x12345"
    result = get_account_logic(account)
    assert result == {
        "account": account
    }, "Should return the correct account dictionary"


def test_account_logic_no_account():
    # Test with no account
    account = None
    with pytest.raises(HTTPException) as excinfo:
        get_account_logic(account)
    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == "Account not initialized."
