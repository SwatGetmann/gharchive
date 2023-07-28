from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.decorators import task

from datetime import datetime, timedelta
import requests


#### DAGs & Tasks

start_date = datetime(2023, 7, 20)

spark_master = "spark://spark:7077"
spark_app_name = "GH Archive Test"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": start_date,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="gharchive_02_spark",
    description="""
    Processor for GitHubArchive.
    # - Crawls the archive for the past day (and even catches up), or for selected day
    # - Stores the file in shared Spark resource folder
    - Passes the file paths to Spark for aggregations
    - Runs Spark job with the crawled file
    # - Saves the result from Spark into Clcikhouse
    # - Removes the file after completion
    """,
    default_args=default_args, 
    # schedule_interval=timedelta(hours=1),
    schedule=None,
    catchup=False,
    concurrency=4,
    max_active_runs=1
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="spark_test_job",
        application="/opt/spark/app/gharchive_v1.py", # Spark application path created in airflow and spark cluster
        name=spark_app_name,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        application_args=['{{ dag_run.conf.get("date") }}']
    )
