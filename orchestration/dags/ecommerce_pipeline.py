from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

def run_ingestion():
    sys.path.insert(0, "/opt/airflow/project")
    from ingestion.ingest import main
    main()

default_args = {
    "owner": "ecommerce_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="End to end ecommerce data pipeline",
    # schedule_interval="0 0 * * *",
    schedule_interval=None,
    start_date=datetime(2026, 2, 25),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=run_ingestion,
    )

    dbt_staging_task = BashOperator(
        task_id="dbt_run_staging",
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin
        cd /opt/***/project/ecommerce_pipeline
        dbt run --select staging.*
        """
    )

    dbt_test_staging_task = BashOperator(
        task_id="dbt_test_staging",
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin
        cd /opt/***/project/ecommerce_pipeline
        dbt test --select staging.*
        """
    )

    dbt_marts_task = BashOperator(
        task_id="dbt_run_marts",
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin
        cd /opt/***/project/ecommerce_pipeline
        dbt run --select marts.*
        """
    )

    dbt_test_marts_task = BashOperator(
        task_id="dbt_test_marts",
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin
        cd /opt/***/project/ecommerce_pipeline
        dbt test --select marts.*
        """
    )

    ingest_task >> dbt_staging_task >> dbt_test_staging_task >> dbt_marts_task >> dbt_test_marts_task
