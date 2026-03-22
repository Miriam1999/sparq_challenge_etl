FROM apache/airflow:2.7.1
USER airflow
RUN pip install --no-cache-dir "pyarrow>=10.0.1,<10.1.0" "apache-airflow-providers-snowflake==3.3.0"