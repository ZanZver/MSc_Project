import polars as pl
import os


def transform_data(size: str) -> str:
    df = pl.read_json(f"../Data/Extract/{size}/data.json")
    output_parquet_path = f"../Data/Transform/{size}/data.parquet"
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    df.write_parquet(output_parquet_path)
