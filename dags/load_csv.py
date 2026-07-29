import pandas as pd
from sqlalchemy import create_engine
import os

# csv location (inside the Airflow container)
CSV_FILE = "/opt/airflow/data/sales.csv"

# MySQL connection
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "host.docker.internal")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "airflow_dbt_lab")

# create SQLAlchemy engine
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Read the CSV
df = pd.read_csv(CSV_FILE)

# Replace existing data
with engine.begin() as conn:
    conn.exec_driver_sql("TRUNCATE TABLE sales_raw")

# Load into MySQL
df.to_sql(
    "sales_raw",
    con=engine,
    if_exists="append",
    index=False,
)

print(f"Loaded {len(df)} rows into sales_raw.")