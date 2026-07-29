FROM apache/airflow:3.3.0

USER root
RUN python3 -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-mysql protobuf==4.25.3

USER airflow

RUN pip install --no-cache-dir pandas sqlalchemy pymysql
