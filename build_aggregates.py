import sqlite3
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "insurance.db"
OUT_DIR = ROOT / "outputs"

OUT_DIR.mkdir(exist_ok=True)


def run_query(query, name):
    """Run SQL query and save result as CSV."""

    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn)

    output_file = OUT_DIR / f"{name}.csv"
    df.to_csv(output_file, index=False)

    print(f"Saved: {output_file}")


queries = {

    "avg_charges_by_region":
        "SELECT * FROM v_avg_charges_region ORDER BY avg_charges DESC",

    "smoker_analysis":
        "SELECT * FROM v_smoker_agg",

    "age_group_analysis":
        "SELECT * FROM v_age_group ORDER BY avg_charges DESC",

    "bmi_category_analysis":
        "SELECT * FROM v_bmi_bin ORDER BY avg_charges DESC",
}


if __name__ == "__main__":

    for name, query in queries.items():
        run_query(query, name)

    print("All aggregate datasets generated.")
