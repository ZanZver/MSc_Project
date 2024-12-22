import polars as pl

# File paths
json_file = "../../Data/Extract/Small/data.json"  # Replace with your JSON file path
parquet_file = "../../Data/Transform/Small/data.parquet"  # Desired Parquet output file path

# Read JSON file into a DataFrame
df = pl.read_json(json_file)

# Save the DataFrame as a Parquet file
df.write_parquet(parquet_file)

