from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import sys
import requests
import json
import os

def slack_failure_alert(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    exception = context.get('exception')
    error_message = str(exception) if exception else 'No error message available'
    
    message = {
        "text": f"""
❌ *Pipeline Failed!*
*DAG:* {dag_id}
*Task:* {task_id}
*Time:* {execution_date}
*Error:* {error_message}
*Logs:* {log_url}
*Note:* Check the logs for more details.
        """
    }
    
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if webhook_url:
        requests.post(webhook_url, data=json.dumps(message))
    else:
        print("SLACK_WEBHOOK_URL not set. Unable to send alert.")

def run_ingestion():
    sys.path.insert(0, "/opt/airflow/project")
    from ingestion.ingest import main
    main()

default_args = {
    "owner": "ecommerce_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_alert
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

    dbt_snapshot_task = BashOperator(
        task_id='dbt_snapshot',
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin
        cd /opt/***/project/ecommerce_pipeline
        dbt snapshot
        """
   )

    dbt_staging_task = BashOperator(
        task_id="dbt_run_staging",
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin
        cd /opt/***/project/ecommerce_pipeline
        dbt run --select staging.*
        """
    ) ---Correct

    # dbt_staging_task = BashOperator(
    # task_id="dbt_run_staging",
    # bash_command="""
    # export PATH=$PATH:/home/airflow/.local/bin
    # cd /opt/airflow/project/ecommerce_pipeline
    # dbt run --select staging.* --invalid-flag
    # """
    # ) # This will cause the staging task to fail, triggering the Slack alert and allowing us to verify that the alerting mechanism works as expected.

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

    ingest_task >> dbt_snapshot_task >> dbt_staging_task >> dbt_test_staging_task >> dbt_marts_task >> dbt_test_marts_task
