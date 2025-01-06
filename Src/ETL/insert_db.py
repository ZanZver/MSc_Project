import psycopg2
from psycopg2.extras import execute_values
import polars as pl

DB_NAME = "testdb"
DB_USER = "admin"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "6432"


def load_and_prepare_data(parquet_file_path: str) -> pl.DataFrame:  # pragma: no cover
    """
    Load and process the Parquet data file, expanding the `full_vehicleInfo` column.
    """
    # Read the Parquet file
    df = pl.read_parquet(parquet_file_path)

    # Cast 'full_vehicleInfo' to Struct type and unnest
    return df.with_columns(pl.col("full_vehicleInfo").cast(pl.Struct)).unnest(
        "full_vehicleInfo"
    )


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
    except Exception as e:  # pragma: no cover
        print(f"An error occurred while creating the table: {e}")
    finally:
        if conn:
            conn.close()


def insert_data_into_db(df: pl.DataFrame, table_name: str):  # pragma: no cover
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


def db_insert_data(size):
    table_name = "vehicles"

    df = load_and_prepare_data(f"../Data/Transform/{size}/data.parquet")

    create_table_from_df(table_name, df)

    insert_data_into_db(df, table_name)
