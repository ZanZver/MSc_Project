from web3 import Web3
import polars as pl
import json


def create_connection(node_ip="http://127.0.0.1:8545"):  # pragma: no cover
    # Connect to the local Ethereum node
    w3 = Web3(Web3.HTTPProvider(node_ip))

    # Check connection
    if not w3.is_connected():
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


def store_data_in_blockchain(data_to_store, account, w3):
    # Convert the data to hexadecimal (Ethereum stores data in hex)
    data_hex = Web3.to_hex(text=json.dumps(data_to_store))

    # Create and send a transaction
    tx = {
        "from": account,
        "to": None,  # Null address for data-only transaction
        "value": 0,  # No Ether transfer
        "gas": 3000000,  # Gas limit
        "gasPrice": w3.to_wei("20", "gwei"),
        "data": data_hex,
    }
    tx_hash = w3.eth.send_transaction(tx)
    return tx_hash


def store_data(data, account, w3, tx_hashes):
    for record in data:
        tx_hash = store_data_in_blockchain(record, account, w3)
        # print(f"Stored record with transaction hash: {tx_hash.hex()}")
        tx_hashes.append(tx_hash)
    return tx_hashes


def bc_insert_data(size):
    tx_hashes = []
    account, w3 = create_connection()

    data = read_data(f"../Data/Transform/{size}/data.parquet")

    store_data(data, account, w3, tx_hashes)
