import pandas as pd
from sqlalchemy import create_engine

# Setup DB connection
engine = create_engine("sqlite:///sales_data.db")

def clean_sales_data(df):
    # Drop duplicate rows
    df = df.drop_duplicates()
    print (df.head())
    # Handle missing values
    for col in ['Quantity', 'UnitPrice']:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].mean())

    # Clean 'date' column
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df = df.dropna(subset=['OrderDate'])

    # Clean 'quantity' (remove extreme outliers)
    df = df[df['Quantity'].between(1, 1000)]

    # Remove rows with invalid or negative prices
    df = df[df['UnitPrice'] > 0]

    return df

def main():
    print("Loading CSV from raw_data folder...")
    df = pd.read_csv("raw_data/sales_data_1year_20k.csv")

    print(f"Original rows: {len(df)}")

    clean_df = clean_sales_data(df)

    print(f"Cleaned rows: {len(clean_df)}")

    # Save cleaned version to DB
    clean_df.to_sql("clean_sales_data", con=engine, if_exists="replace", index=False)
    print("Cleaned data saved to table: clean_sales_data")

if __name__ == "__main__":
    main()