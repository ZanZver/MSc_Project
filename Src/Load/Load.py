import polars as pl

# File paths
input_parquet = "../../Data/Transform/Small/data.parquet"  # Replace with your input Parquet file path
output_parquet = "../../Data/Load/Small/data.parquet"  # Desired output Parquet file path

# Read the Parquet file into a DataFrame
df = pl.read_parquet(input_parquet)

# Save the DataFrame to a new Parquet file
df.write_parquet(output_parquet)