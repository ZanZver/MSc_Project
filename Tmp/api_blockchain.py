from web3 import Web3
import polars as pl
import json


def create_connection(node_ip="http://127.0.0.1:8545"):
    # Connect to the local Ethereum node
    w3 = Web3(Web3.HTTPProvider(node_ip))

    # Check connection
    if not w3.isConnected():
        raise Exception("Failed to connect to the Ethereum node.")

    # Use the first account for transactions (ensure it's unlocked)
    account = w3.eth.accounts[0]

    return account, w3


def read_data(data_path="data.parquet"):
    """
    Load data from a Parquet file using Polars.

    Args:
        data_path (str): Path to the Parquet file.

    Returns:
        list[dict]: Data loaded from the Parquet file, converted to a list of dictionaries.
    """
    # Load data from a Parquet file into a Polars DataFrame
    df = pl.read_parquet(data_path)

    # Convert Polars DataFrame to a list of dictionaries
    return df.to_dicts()


# Function to store data in blockchain
def store_data_in_blockchain(data_to_store, account, w3):
    """
    Store data on the blockchain in a data-only transaction.

    Args:
        data_to_store (dict): Data to be stored, serialized as JSON.
        account (str): The account sending the transaction.
        w3 (Web3): Web3 instance.

    Returns:
        str: Transaction hash of the data-only transaction.
    """
    # Convert the data to hexadecimal (Ethereum stores data in hex)
    data_hex = Web3.toHex(text=json.dumps(data_to_store))

    # Create and send a transaction
    tx = {
        "from": account,
        "to": None,  # Null address for data-only transaction
        "value": 0,  # No Ether transfer
        "gas": 3000000,  # Gas limit
        "gasPrice": w3.toWei("20", "gwei"),
        "data": data_hex,
    }
    tx_hash = w3.eth.send_transaction(tx)
    return tx_hash


# Function to retrieve data from blockchain
def retrieve_data_from_blockchain(w3, tx_hash):
    """
    Retrieve the `input` field from a transaction on the blockchain and decode it.

    Args:
        w3 (Web3): Web3 instance.
        tx_hash (str): The transaction hash.

    Returns:
        str: Decoded text data from the transaction input field.
    """
    # Get the transaction details
    tx = w3.eth.get_transaction(tx_hash)

    # Check the type of `input` field and handle it
    tx_input = tx["input"]
    if isinstance(tx_input, str):  # Already a string
        decoded_data = Web3.toText(hexstr=tx_input)
    else:  # HexBytes, convert to hex string first
        decoded_data = Web3.toText(hexstr=tx_input.hex())

    return decoded_data


# Store data
def store_data(data, account, w3, tx_hashes):
    """
    Store multiple records on the blockchain.

    Args:
        data (list[dict]): List of data records to store.
        account (str): The account sending the transactions.
        w3 (Web3): Web3 instance.
        tx_hashes (list[str]): List to append transaction hashes to.

    Returns:
        list[str]: List of transaction hashes.
    """
    # print(type(account))  # Should be <class 'str'>
    # print(type(w3))       # Should be <class 'web3.main.Web3'>
    for record in data:
        try:
            tx_hash = store_data_in_blockchain(record, account, w3)
            # print(f"Stored record with transaction hash: {tx_hash.hex()}")
            tx_hashes.append(tx_hash.hex())
        except Exception as e:
            print("Error while storing record:")
            print(record)
            print(f"Exception: {e}")

    return tx_hashes


# Retrieve data
def retrive_data(w3, tx_hashes):
    for tx_hash in tx_hashes:
        try:
            # Ensure tx_hash is bytes if needed
            if isinstance(tx_hash, str):
                # If tx_hash is a string, decode to bytes
                tx_hash = (
                    bytes.fromhex(tx_hash[2:])
                    if tx_hash.startswith("0x")
                    else bytes.fromhex(tx_hash)
                )

            stored_data = retrieve_data_from_blockchain(w3, tx_hash)

            # print(tx_hash.hex())  # tx_hash is now bytes, so .hex() works
            print(f"Retrieved data from transaction {tx_hash.hex()}: {stored_data}")
            # print(type(tx_hash))
        except Exception as e:
            print(f"Error processing transaction {tx_hash}: {e}")
            print(type(tx_hash))
            break


# def retrive_data(w3, tx_hashes):
#     for tx_hash in tx_hashes:
#         stored_data = retrieve_data_from_blockchain(w3, tx_hash)
#         try:
#             print(f"Retrieved data from transaction {tx_hash.hex()}: {stored_data}")
#         except Exception as e:
#             print("Error 1:")
#             print(e)
#             print(tx_hash)
#             print(stored_data)
#             break
#         except AttributeError as a:
#             print("Error 2:")
#             print(tx_hash)
#             print(stored_data)
#             break


# Function to append new data to the blockchain
def append_data_to_blockchain(record, Web3, account, w3):
    """
    Add a new record (or update) to the blockchain.

    Args:
        record (dict): The record to store or update.
        Web3 (class): Web3 class for blockchain interactions.
        account (str): The account initiating the transaction.
        w3 (Web3): An instance of the Web3 connection.

    Returns:
        str: Transaction hash of the appended record.
    """
    # Convert the data to hexadecimal (Ethereum stores data in hex)
    data_hex = Web3.toHex(text=json.dumps(record))

    # Create and send a transaction
    tx = {
        "from": account,
        "to": None,  # Null address for data-only transaction
        "value": 0,  # No Ether transfer
        "gas": 3000000,  # Gas limit
        "gasPrice": Web3.toWei("20", "gwei"),  # Corrected to use Web3.toWei
        "data": data_hex,
    }
    # print(type(account))  # Should be <class 'str'>
    # print(type(w3))       # Should be <class 'web3.main.Web3'>
    tx_hash = w3.eth.send_transaction(tx)
    return tx_hash


# Function to fetch and decode the latest transaction for a specific key
def get_latest_record(key, w3, key_field="vin"):
    """
    Fetch the latest record for a given key from the blockchain.

    Args:
        key (str): The unique identifier for the record.
        key_field (str): The field name used as the key in the record.

    Returns:
        dict or None: The latest record if found, otherwise None.
    """
    latest_record = None

    # Iterate through the blockchain's transactions
    # print("<<<<< 0 >>>>>")
    for block_number in range(w3.eth.block_number + 1):
        # print("<<<<< 1 >>>>>")
        block = w3.eth.get_block(block_number, full_transactions=True)
        for tx in block.transactions:
            # print("<<<<< 2 >>>>>")
            if tx.to is None:  # Data-only transactions
                # print("<<<<< 3 >>>>>")
                # Handle tx.input based on its type
                data_hex = tx.input if isinstance(tx.input, str) else tx.input.hex()
                try:
                    # print("<<<<< 4 >>>>>")
                    record = json.loads(Web3.toText(hexstr=data_hex))
                    if record.get(key_field) == key:
                        # print("<<<<< 5 >>>>>")
                        latest_record = record
                except Exception as e:
                    print(f"Error decoding transaction: {e}")

    return latest_record


def get_record_history(w3, key, key_field="vin"):
    """
    Fetch the history of a specific record from the blockchain.

    Args:
        key (str): The unique identifier for the record.
        key_field (str): The field name used as the key in the record.

    Returns:
        list[dict]: A list of all records matching the key in their history.
    """
    history = []

    # Iterate through all blocks
    for block_number in range(w3.eth.block_number + 1):
        block = w3.eth.get_block(block_number, full_transactions=True)
        for tx in block.transactions:
            if tx.to is None:  # Data-only transactions
                try:
                    # Decode the transaction input (data field)
                    # print("~~~~~~~~~~ 1.1 ~~~~~~~~~~")
                    data_hex = tx.input if isinstance(tx.input, str) else tx.input.hex()
                    # print(f"Decoded data: {data_hex}")
                    record = json.loads(Web3.toText(hexstr=data_hex))
                    # print("~~~~~~~~~~ 1.3 ~~~~~~~~~~")
                    # Check if the transaction contains the relevant key
                    if record.get(key_field) == key:
                        history.append(record)
                except Exception as e:
                    print(f"Error decoding transaction: {e}")

    return history


# Function to append a "deleted" record to the blockchain
def delete_record(key, w3, account, key_field="vin"):
    """
    Mark a record as deleted on the blockchain.

    Args:
        key (str): The unique identifier for the record to delete.
        key_field (str): The field name used as the key in the record.

    Returns:
        str: Transaction hash of the deletion transaction.
    """
    # Create the deletion record
    deletion_record = {key_field: key, "deleted": True}

    # Convert the data to hexadecimal (Ethereum stores data in hex)
    data_hex = Web3.toHex(text=json.dumps(deletion_record))

    # Create and send a transaction
    tx = {
        "from": account,
        "to": None,  # Null address for data-only transaction
        "value": 0,  # No Ether transfer
        "gas": 3000000,  # Gas limit
        "gasPrice": w3.toWei("20", "gwei"),
        "data": data_hex,
    }
    # tx_hash =
    w3.eth.send_transaction(tx)
    # return tx_hash


def test1(w3):
    print("~~~~~~~~~~ 1.1 ~~~~~~~~~~")
    # Retrieve the latest record for a specific VIN
    vin = "82HFE9767U326DEZ2"
    latest_record = get_latest_record(vin, w3)
    print(f"Latest record for VIN {vin}: {latest_record}")

    print("~~~~~~~~~~ 1.2 ~~~~~~~~~~")
    # Retrieve the latest record for a specific VIN
    vin = "H1AUMH0D9M76R7NNG"
    latest_record = get_latest_record(vin, w3)
    print(f"Latest record for VIN {vin}: {latest_record}")

    print("~~~~~~~~~~ 1.3 ~~~~~~~~~~")
    # Retrieve the latest record for a specific VIN
    vin = "JPC53EJ63E7RHWPAP"
    latest_record = get_latest_record(vin, w3)
    print(f"Latest record for VIN {vin}: {latest_record}")

    print("~~~~~~~~~~ 1.4 ~~~~~~~~~~")
    # Retrieve the latest record for a specific VIN
    license_plate = "GV19IWV"
    latest_record = get_latest_record(license_plate, w3, "license_plate")
    print(f"Latest record for license plate {license_plate}: {latest_record}")


def test2(w3, account):
    # Example Usage
    # Append updated record to blockchain
    updated_record = {
        "vin": "82HFE9767U326DEZ2",
        "license_plate": "WU37 WRN",
        "vehicle_make": "Mitsubishi",
        "vehicle_model": "Outlander",
        "vehicle_year": 2000,
        "full_vehicleInfo": {
            "Year": 2000,
            "Make": "Mitsubishi",
            "Model": "Outlander",
            "Category": "SUV",
        },
        "vehicle_category": "SUV",
        "vehicle_make_model": "Mitsubishi Outlander",
        "vehicle_year_make_model": "2000 Mitsubishi Outlander",
        "vehicle_year_make_model_cat": "2000 Mitsubishi Outlander (SUV)",
    }
    print("~~~~~~~~~~ 2.1 ~~~~~~~~~~")
    tx_hash = append_data_to_blockchain(updated_record, Web3, account, w3)
    print(f"Updated record added to blockchain with transaction hash: {tx_hash.hex()}")

    print("~~~~~~~~~~ 2.2 ~~~~~~~~~~")
    # Retrieve the latest record for a specific VIN
    vin = "82HFE9767U326DEZ2"
    latest_record = get_latest_record(vin, w3)
    print(f"Latest record for VIN {vin}: {latest_record}")


def test3(w3):
    vin = "82HFE9767U326DEZ2"
    history = get_record_history(w3, vin)
    print(history)


def test4(w3, account):
    print("~~~~~~~~~~ 4.1 ~~~~~~~~~~")
    # Get current status of the record
    vin = "82HFE9767U326DEZ2"
    latest_record = get_latest_record(vin, w3)
    print(f"Latest record for VIN {vin}: {latest_record}")

    print("~~~~~~~~~~ 4.2 ~~~~~~~~~~")
    # Remove the record
    vin = "82HFE9767U326DEZ2"
    delete_record(vin, w3, account)

    print("~~~~~~~~~~ 4.3 ~~~~~~~~~~")
    # Get current status of the record
    vin = "82HFE9767U326DEZ2"
    latest_record = get_latest_record(vin, w3)
    print(f"Latest record for VIN {vin}: {latest_record}")

    print("~~~~~~~~~~ 4.4 ~~~~~~~~~~")
    # Get historical status of the record
    vin = "82HFE9767U326DEZ2"
    print(get_record_history(w3, vin))


def main():
    # Create connection
    tx_hashes = []
    account, w3 = create_connection()

    print("~~~~~~~~~~ 0.1 ~~~~~~~~~~")

    # Read data from parquet
    data = read_data("../Data/Transform/Small/data.parquet")

    print("~~~~~~~~~~ 0.2 ~~~~~~~~~~")

    # Save data to blockchain
    tx_hashes = store_data(data, account, w3, tx_hashes)

    # Retrieve data from blockchain
    retrive_data(w3, tx_hashes)

    print("~~~~~~~~~~ 1 ~~~~~~~~~~")
    test1(w3)

    print("~~~~~~~~~~ 2 ~~~~~~~~~~")
    test2(w3, account)

    print("~~~~~~~~~~ 3 ~~~~~~~~~~")
    test3(w3)

    print("~~~~~~~~~~ 4 ~~~~~~~~~~")
    test4(w3, account)


if __name__ == "__main__":
    main()
