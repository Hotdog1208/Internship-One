import pandas as pd
from sqlalchemy import create_engine

# Connect to your SQLite DB
engine = create_engine("sqlite:///sales_data.db")

def add_features(df):
    # Sort by date just in case
    df = df.sort_values("OrderDate")

    # Extract time-based features
    df["year"] = df["OrderDate"].dt.year
    df["month"] = df["OrderDate"].dt.month
    df["day"] = df["OrderDate"].dt.day
    df["day_of_week"] = df["OrderDate"].dt.dayofweek  # Monday = 0

    # Lag feature: previous day's quantity
    df["lag_1"] = df["Quantity"].shift(1)

    # Rolling window features (3-day window)
    df["rolling_mean_3"] = df["Quantity"].rolling(window=3).mean()
    df["rolling_std_3"] = df["Quantity"].rolling(window=3).std()

    # Fill NaNs from lag/rolling with zeros or backfill
    df = df.fillna(0)

    return df

def main():
    print("Loading cleaned data...")
    df = pd.read_sql_table("clean_sales_data", con=engine)

    # Convert date column to datetime if not already
    df["OrderDate"] = pd.to_datetime(df["OrderDate"])

    print("Engineering features...")
    feature_df = add_features(df)

    print("Saving features to 'features_sales_data' table...")
    feature_df.to_sql("features_sales_data", con=engine, if_exists="replace", index=False)

    print("Feature engineering complete!")

if __name__ == "__main__":
    main()