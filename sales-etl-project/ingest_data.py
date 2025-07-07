import os
import pandas as pd
from sqlalchemy import create_engine

# connect to SQLite - Creates the DB if it doesnt already exist
db_path = "sales_data.db"
engine = create_engine(f"sqlite:///{db_path}")

# raw data directory
DATA_DIR = "C:/Users/supri/sales-etl-project/raw_data"

def load_file(file_path):
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    elif file_path.endswith(".json"):
        return pd.read_json(file_path)
    return None # for unsupported file type

def ingest_files():
    files = os.listdir(DATA_DIR)
    if not files:
        print("No files found in raw_data/")
        return
    
    for file in files:
        path = os.path.join(DATA_DIR, file)
        df = load_file(path)

        if df is None:
            print(f"Skipping {file} (unsupported format)")
            continue

        table = os.path.splitext(file)[0]
        try:
            df.to_sql(name=table, con=engine, if_exists="replace", index=False)
            print(f"Loaded {file} -> Table: {table}")
        except Exception as e:
            print(f"Failed to load {file}: {e}")

if __name__ == "__main__":
    print("Starting ingestion...")
    ingest_files()
    print("Done.")