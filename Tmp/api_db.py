import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import polars as pl
from psycopg2 import OperationalError, sql
from contextlib import contextmanager

DB_NAME = "testdb"
DB_USER = "admin"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "6432"


@contextmanager
def get_db_connection():
    """Context manager for PostgreSQL database connection."""
    conn = None
    try:
        # Establish the connection using environment variables
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        # Yield the connection to be used in the 'with' block
        yield conn
    except OperationalError as e:
        print(f"An error occurred while connecting to the database: {e}")
        raise
    finally:
        if conn:
            conn.close()


def test_db_connection():
    """Test function to verify database connection."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Execute a simple query to test the connection
                cur.execute(sql.SQL("SELECT 1"))
                result = cur.fetchone()
                if result:
                    print("Database connection successful.")
                else:
                    print("Failed to retrieve data from the database.")
    except Exception as e:
        print(f"Test failed: {e}")


def load_and_prepare_data(parquet_file_path: str) -> pl.DataFrame:
    """
    Load and process the Parquet data file, expanding the `full_vehicleInfo` column.
    """
    # Read the Parquet file
    df = pl.read_parquet(parquet_file_path)

    with pl.Config(tbl_cols=-1):
        print(df)

    # Ensure 'full_vehicleInfo' column is not empty or invalid
    if "full_vehicleInfo" not in df.columns:
        raise ValueError("The 'full_vehicleInfo' column is missing from the data.")

    # Filter out rows with null or invalid `full_vehicleInfo`
    df = df.filter(pl.col("full_vehicleInfo").is_not_null())

    # Expand `full_vehicleInfo` manually by selecting its fields
    try:
        expanded_df = df.with_columns(
            [
                pl.col("full_vehicleInfo").struct.field("Year").alias("vehicle_year"),
                pl.col("full_vehicleInfo").struct.field("Make").alias("vehicle_make"),
                pl.col("full_vehicleInfo").struct.field("Model").alias("vehicle_model"),
                pl.col("full_vehicleInfo")
                .struct.field("Category")
                .alias("vehicle_category"),
            ]
        )
        return expanded_df.drop("full_vehicleInfo")
    except Exception as e:
        print(f"Error during manual expansion: {e}")
        raise


def insert_data_into_db(df: pl.DataFrame, table_name: str):
    """
    Insert data from a Polars DataFrame into a PostgreSQL table.
    """
    # Convert Polars DataFrame to list of tuples
    records = df.to_dicts()

    # Use the first record to generate column names dynamically
    columns = list(records[0].keys())
    rows = [tuple(record.values()) for record in records]

    # Database connection
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with conn.cursor() as cur:
            # Create an insert query dynamically
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
            execute_values(cur, insert_query, rows)
            conn.commit()
            print(f"{len(rows)} records successfully inserted into {table_name}.")
    except Exception as e:
        print(f"An error occurred while inserting data: {e}")
    finally:
        if conn:
            conn.close()


def map_polars_to_postgres_types(polars_dtype):
    """
    Map Polars data types to PostgreSQL data types.
    """
    type_mapping = {
        pl.Int32: "INTEGER",
        pl.Int64: "BIGINT",
        pl.Float32: "REAL",
        pl.Float64: "DOUBLE PRECISION",
        pl.Utf8: "TEXT",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Datetime: "TIMESTAMP",
        pl.List: "JSONB",  # If lists are used, JSONB is a good fit
    }
    return type_mapping.get(polars_dtype, "TEXT")  # Default to TEXT for unknown types


def create_table_from_df(table_name: str, df: pl.DataFrame):
    """
    Create a PostgreSQL table based on the schema of a Polars DataFrame.
    """
    # Generate column definitions based on DataFrame schema
    columns = [
        f"{col_name} {map_polars_to_postgres_types(dtype)}"
        for col_name, dtype in zip(df.columns, df.dtypes)
    ]
    columns_sql = ", ".join(columns)

    # Construct CREATE TABLE statement
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    """

    # Connect to PostgreSQL and execute the query
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating the table: {e}")
    finally:
        if conn:
            conn.close()


def retrieve_data_from_db(query: str, params: tuple = ()) -> list:
    """
    Retrieve data from the PostgreSQL database based on a query.

    Args:
        query (str): The SQL query to execute.
        params (tuple): Parameters for the SQL query.

    Returns:
        list: A list of dictionaries containing the query results.
    """
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Execute the query
            cur.execute(query, params)
            # Fetch all results
            # Convert the results to a Polars DataFrame
            return pl.DataFrame(cur.fetchall())
    except Exception as e:
        print(f"An error occurred while retrieving data: {e}")
        return []
    finally:
        if conn:
            conn.close()


def update_record_in_db(
    table_name: str, update_values: dict, condition: str, condition_params: tuple
):
    """
    Update records in a PostgreSQL table.

    Args:
        table_name (str): The name of the table to update.
        update_values (dict): A dictionary of column-value pairs to update.
        condition (str): The WHERE clause to specify which records to update.
        condition_params (tuple): Parameters for the WHERE clause.

    Returns:
        int: The number of rows affected.
    """
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with conn.cursor() as cur:
            # Generate the SET part of the SQL query dynamically
            set_clause = ", ".join([f"{col} = %s" for col in update_values.keys()])
            update_query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"

            # Combine the values to update and the WHERE clause parameters
            query_params = tuple(update_values.values()) + condition_params

            # Execute the update query
            cur.execute(update_query, query_params)
            conn.commit()

            # Return the number of rows affected
            return cur.rowcount
    except Exception as e:
        print(f"An error occurred while updating the record: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def delete_record_from_db(
    table_name: str, condition: str, condition_params: tuple
) -> int:
    """
    Delete records from a PostgreSQL table.

    Args:
        table_name (str): The name of the table from which to delete records.
        condition (str): The WHERE clause to specify which records to delete.
        condition_params (tuple): Parameters for the WHERE clause.

    Returns:
        int: The number of rows affected (deleted).
    """
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        with conn.cursor() as cur:
            # Construct the DELETE query
            delete_query = f"DELETE FROM {table_name} WHERE {condition}"

            # Execute the DELETE query
            cur.execute(delete_query, condition_params)
            conn.commit()

            # Return the number of rows deleted
            return cur.rowcount
    except Exception as e:
        print(f"An error occurred while deleting the record: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def test1():
    data = retrieve_data_from_db("SELECT * FROM vehicles;", "")
    with pl.Config(tbl_cols=-1):
        print(data)


def test2():
    # Get current status of a car
    data = retrieve_data_from_db(
        query="SELECT * FROM vehicles WHERE vin = %s;", params=("82HFE9767U326DEZ2",)
    )
    with pl.Config(tbl_cols=-1):
        print(data)

    # Update the car status
    table_name = "vehicles"
    update_values = {"vehicle_make": "Toyota", "vehicle_model": "Camry"}
    condition = "vin = %s"
    condition_params = ("82HFE9767U326DEZ2",)
    rows_updated = update_record_in_db(
        table_name, update_values, condition, condition_params
    )
    print(f"Number of rows updated: {rows_updated}")

    # Get updated status of a car
    data = retrieve_data_from_db(
        query="SELECT * FROM vehicles WHERE vin = %s;", params=("82HFE9767U326DEZ2",)
    )
    with pl.Config(tbl_cols=-1):
        print(data)


def test3():
    pass


def test4():
    # Get current status of a car
    data = retrieve_data_from_db(
        query="SELECT * FROM vehicles WHERE vin = %s;", params=("82HFE9767U326DEZ2",)
    )
    with pl.Config(tbl_cols=-1):
        print(data)

    # Remove the car
    table_name = "vehicles"
    condition = "vin = %s"
    condition_params = ("82HFE9767U326DEZ2",)
    rows_deleted = delete_record_from_db(table_name, condition, condition_params)
    print(f"Number of rows deleted: {rows_deleted}")

    # Get updated status of a car
    data = retrieve_data_from_db(
        query="SELECT * FROM vehicles WHERE vin = %s;", params=("82HFE9767U326DEZ2",)
    )
    with pl.Config(tbl_cols=-1):
        print(data)


def main():
    # Test connection
    test_db_connection()

    # Prepare data
    parquet_file_path = "../Data/Transform/Small/data.parquet"
    table_name = "vehicles"

    df = load_and_prepare_data(parquet_file_path)

    # Create table
    create_table_from_df(table_name, df)

    # Insert data
    insert_data_into_db(df, table_name)

    print("~~~~~~~~~~ 1 ~~~~~~~~~~")
    test1()

    print("~~~~~~~~~~ 2 ~~~~~~~~~~")
    test2()

    # print("~~~~~~~~~~ 3 ~~~~~~~~~~")
    # test3(w3)

    print("~~~~~~~~~~ 4 ~~~~~~~~~~")
    test4()


if __name__ == "__main__":
    main()
