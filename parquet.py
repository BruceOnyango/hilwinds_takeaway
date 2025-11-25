import pandas as pd

# Load the Parquet file
df = pd.read_parquet("outputs/clean_data.parquet")

# Show the first few rows
print(df.head())

# Check column names
print(df.columns)

# Summary
print(df.info())
