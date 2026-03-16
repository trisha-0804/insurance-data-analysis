import subprocess
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

print("STEP 1: Load CSV into SQLite")

subprocess.run(
    ["python", str(ROOT / "src" / "csv_to_sqlite.py")],
    check=True
)

print("STEP 2: Create SQL Views")

sql_file = ROOT / "sql" / "create_views.sql"
db_path = ROOT / "data" / "insurance.db"

with sqlite3.connect(db_path) as conn:
    with open(sql_file) as f:
        conn.executescript(f.read())

print("Views created successfully")


print("STEP 3: Generate Aggregate CSVs")

subprocess.run(
    ["python", str(ROOT / "src" / "build_aggregates.py")],
    check=True
)

print("Pipeline completed successfully!")
