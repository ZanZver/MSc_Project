import polars as pl
import os


def load_data(size: str) -> str:
    df = pl.read_parquet(f"../Data/Transform/{size}/data.parquet")
    output_parquet_path = f"../Data/Load/{size}/data.parquet"
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    df.write_parquet(output_parquet_path)
