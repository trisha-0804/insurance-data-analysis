# insurance-data-analysis
# src/csv_to_sqlite.py
import pandas as pd
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_CSV = ROOT / "data" / "insurance.csv"
DB_PATH = ROOT / "data" / "insurance.db"

def normalize_df(df):
    df.columns = [c.strip().lower() for c in df.columns]
    # ensure expected dtypes
    df['age'] = df['age'].astype(int)
    df['bmi'] = pd.to_numeric(df['bmi'], errors='coerce')
    df['children'] = df['children'].astype(int)
    df['charges'] = pd.to_numeric(df['charges'], errors='coerce')
    df['sex'] = df['sex'].astype(str)
    df['region'] = df['region'].astype(str)
    df['smoker'] = df['smoker'].astype(str)
    return df

def main():
    df = pd.read_csv(DATA_CSV)
    df = normalize_df(df)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('insurance', conn, if_exists='replace', index=False)
    conn.close()
    print("Wrote", DB_PATH)
    
# SQL: useful queries + views (sql/create_views.sql)
-- sql/create_views.sql

-- 1. Average charges by region
DROP VIEW IF EXISTS v_avg_charges_region;
CREATE VIEW v_avg_charges_region AS
SELECT region,
       ROUND(AVG(charges),2) AS avg_charges,
       COUNT(*) AS cnt
FROM insurance
GROUP BY region;

-- 2. Smoker vs Non-Smoker aggregates
DROP VIEW IF EXISTS v_smoker_agg;
CREATE VIEW v_smoker_agg AS
SELECT smoker,
       ROUND(AVG(charges),2) AS avg_charges,
       COUNT(*) AS cnt
FROM insurance
GROUP BY smoker;

-- 3. Age bins average charges
DROP VIEW IF EXISTS v_age_group;
CREATE VIEW v_age_group AS
SELECT CASE
         WHEN age < 30 THEN 'below_30'
         WHEN age BETWEEN 30 AND 39 THEN '30_39'
         WHEN age BETWEEN 40 AND 49 THEN '40_49'
         WHEN age BETWEEN 50 AND 59 THEN '50_59'
         ELSE '60_plus'
       END AS age_group,
       ROUND(AVG(charges),2) AS avg_charges,
       COUNT(*) AS cnt
FROM insurance
GROUP BY age_group;

-- 4. BMI categories
DROP VIEW IF EXISTS v_bmi_bin;
CREATE VIEW v_bmi_bin AS
SELECT bmi_cat, ROUND(AVG(charges),2) avg_charges, COUNT(*) cnt
FROM (
  SELECT *,
         CASE
           WHEN bmi < 18.5 THEN 'underweight'
           WHEN bmi BETWEEN 18.5 AND 24.9 THEN 'normal'
           WHEN bmi BETWEEN 25 AND 29.9 THEN 'overweight'
           ELSE 'obese'
         END AS bmi_cat
  FROM insurance
)
GROUP BY bmi_cat;


if __name__ == "__main__":
    main()

# Python helper to build aggregate CSVs (for Power BI or repo) (src/build_aggregates.py)
# src/build_aggregates.py
import sqlite3
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "insurance.db"
OUT = ROOT / "outputs"
OUT.mkdir(exist_ok=True)

def run_query(q, name):
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(q, conn)
    df.to_csv(OUT / f"{name}.csv", index=False)
    print("Wrote", name)

queries = {
    "avg_charges_by_region": "SELECT * FROM v_avg_charges_region ORDER BY avg_charges DESC;",
    "smoker_agg": "SELECT * FROM v_smoker_agg;",
    "age_groups": "SELECT * FROM v_age_group ORDER BY avg_charges DESC;",
    "bmi_bins": "SELECT * FROM v_bmi_bin ORDER BY avg_charges DESC;"
}

if __name__ == "__main__":
    for name, q in queries.items():
        run_query(q, name)

# pipeliner
# src/run_pipeline.py
import subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]

print("Step 1: load csv to sqlite")
subprocess.run(["python", str(ROOT / "src" / "csv_to_sqlite.py")], check=True)

print("Step 2: create SQL views")

# apply create_views.sql
import sqlite3
sql_file = ROOT / "sql" / "create_views.sql"
db = ROOT / "data" / "insurance.db"
with sqlite3.connect(db) as conn:
    sql = open(sql_file).read()
    conn.executescript(sql)
print("Views created")

print("Step 3: build aggregates CSVs")
subprocess.run(["python", str(ROOT / "src" / "build_aggregates.py")], check=True)
print("Pipeline done. Check outputs/ for CSVs.")

SQL queries to run interactively (sql/queries.sql)
    
