import pandas as pd
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_CSV = ROOT / "data" / "insurance.csv"
DB_PATH = ROOT / "data" / "insurance.db"


def normalize_df(df):
    """Clean and normalize dataset."""
    
    df.columns = [c.strip().lower() for c in df.columns]

    df["age"] = df["age"].astype(int)
    df["bmi"] = pd.to_numeric(df["bmi"], errors="coerce")
    df["children"] = df["children"].astype(int)
    df["charges"] = pd.to_numeric(df["charges"], errors="coerce")

    df["sex"] = df["sex"].astype(str)
    df["region"] = df["region"].astype(str)
    df["smoker"] = df["smoker"].astype(str)

    return df


def load_to_database(df):
    """Load dataframe into SQLite database."""

    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("insurance", conn, if_exists="replace", index=False)

    print("Database created at:", DB_PATH)


def main():

    print("Reading CSV file...")
    df = pd.read_csv(DATA_CSV)

    print("Cleaning dataset...")
    df = normalize_df(df)

    print("Loading data into SQLite...")
    load_to_database(df)

    print("Done!")


if __name__ == "__main__":
    main()
