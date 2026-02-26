FROM apache/airflow:2.8.1-python3.10

ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN pip install --no-cache-dir \
    dbt-snowflake==1.11.2 \
    snowflake-connector-python \
    pandas \
    python-dotenv \
    apache-airflow-providers-slack