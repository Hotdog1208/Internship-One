import pandas as pd

# Load the data
df = pd.read_csv("sales_data_1year_20k.csv")

# Show structure and nulls
print("Data Info:")
print(df.info())

# Show the first few rows
print("\nFirst 5 Rows:")
print(df.head())
