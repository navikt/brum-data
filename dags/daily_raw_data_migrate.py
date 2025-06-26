"""DAG to automate the collection of the raw data from other BQ projects to Brum"""
from datetime import datetime
from pendulum import timezone
from airflow import DAG
from dataverk_airflow import python_operator

allowlist = [
    "secretmanager.googleapis.com",
    "bigquery.googleapis.com",

]

with DAG(
    dag_id="daily_raw_data_migrate",
    description="A DAG that migrates data from different BQ projects to the brum project",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 6, 27, tzinfo=timezone("Europe/Oslo")),
    catchup=False
) as dag:
    migrate_bigquery = python_operator(
        dag=dag,
        name="migrate_bigquery",
        repo="navikt/brum-data",
        script_path="src/data_collection/transfer_bq_datasets.py",
        requirements_path="requirements_bq.txt",
        use_uv_pip_install=True,
        slack_channel="#brum-intern",
        allowlist=allowlist
    )

    migrate_bigquery


