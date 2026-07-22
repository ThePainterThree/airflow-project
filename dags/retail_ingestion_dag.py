from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd

RAW_FILE = "/opt/airflow/data/raw/sales_retail.csv"
PROCESSED_FILE = "/opt/airflow/data/processed/sales_clean.csv"

default_args = {
    "owner": "retail_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

def validate_sales_data():
    df = pd.read_csv(RAW_FILE)

    if df.isnull().values.any():
        raise ValueError("Null values detected!")

    if (df["Unit Price"] < 0).any():
        raise ValueError("Negative prices found!")

    print("Validation successful!")

def transform_sales_data():
    df = pd.read_csv(RAW_FILE)

    df["Total_Sales"] = (df["Units Sold"] * df["Unit Price"]).round(2)

    df.to_csv(PROCESSED_FILE, index=False)

    print("Transformation completed!")


with DAG(
    dag_id="retail_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_sales_file",
        filepath=RAW_FILE,
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    validate_task = PythonOperator(
        task_id="validate_sales_data",
        python_callable=validate_sales_data
    )

    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data
    )

    wait_for_file >> validate_task >> transform_task
