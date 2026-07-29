from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="sales_dbt_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    load_csv = BashOperator(
        task_id="load_csv",
        bash_command="python /opt/airflow/dags/load_csv.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="/opt/dbt_venv/bin/dbt run "
                     "--project-dir /opt/airflow/sales_dbt "
                     "--profiles-dir /opt/airflow/dbt_profiles",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="/opt/dbt_venv/bin/dbt test "
                     "--project-dir /opt/airflow/sales_dbt "
                     "--profiles-dir /opt/airflow/dbt_profiles",
    )

    end = EmptyOperator(task_id="end")

start >> load_csv >> dbt_run >> dbt_test >> end