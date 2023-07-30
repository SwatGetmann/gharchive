from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task

from datetime import datetime, timedelta
import pathlib


#### DAGs & Tasks

start_date = datetime(2023, 7, 20)

spark_master = "spark://spark:7077"
spark_app_name = "GH Archive Multicore Processing"

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
    dag_id="gharchive_02b_spark_multicore",
    description="""
    Processor for GitHubArchive.
    # - Crawls the archive for the past day (and even catches up), or for selected day
    # - Stores the file in shared Spark resource folder
    - Passes the file paths to Spark for aggregations
    - Runs Spark job with the crawled file
        - Saves the result from Spark into Clcikhouse
    - Removes the file after completion
    """,
    default_args=default_args, 
    # schedule_interval=timedelta(hours=1),
    schedule=None,
    catchup=False,
    concurrency=4,
    max_active_runs=1,
    tags=['gharchive']
) as dag:

    repos = SparkSubmitOperator(
        task_id="spark_job_repos",
        application="/opt/spark/app/gharchive_repos.py", # Spark application path created in airflow and spark cluster
        name="GH Archive Processing - Repos",
        conn_id="spark_default",
        verbose=1,
        conf={
            "spark.master": spark_master,
        },
        application_args=['{{ dag_run.conf.get("date") }}'],
        jars="/opt/spark/resources/jars/clickhouse-jdbc-0.4.6-all.jar,/opt/spark/resources/jars/clickhouse-native-jdbc-shaded-2.5.4.jar,/opt/spark/resources/jars/clickhouse-spark-runtime-3.3_2.12-0.7.2.jar"
    )

    commits = SparkSubmitOperator(
        task_id="spark_job_commits",
        application="/opt/spark/app/gharchive_commits.py", # Spark application path created in airflow and spark cluster
        name="GH Archive Processing - Commits",
        conn_id="spark_default",
        verbose=1,
        conf={
            "spark.master": spark_master,
        },
        application_args=['{{ dag_run.conf.get("date") }}'],
        jars="/opt/spark/resources/jars/clickhouse-jdbc-0.4.6-all.jar,/opt/spark/resources/jars/clickhouse-native-jdbc-shaded-2.5.4.jar,/opt/spark/resources/jars/clickhouse-spark-runtime-3.3_2.12-0.7.2.jar"
    )

    members = SparkSubmitOperator(
        task_id="spark_job_members",
        application="/opt/spark/app/gharchive_members.py", # Spark application path created in airflow and spark cluster
        name="GH Archive Processing - Members",
        conn_id="spark_default",
        verbose=1,
        conf={
            "spark.master": spark_master,
        },
        application_args=['{{ dag_run.conf.get("date") }}'],
        jars="/opt/spark/resources/jars/clickhouse-jdbc-0.4.6-all.jar,/opt/spark/resources/jars/clickhouse-native-jdbc-shaded-2.5.4.jar,/opt/spark/resources/jars/clickhouse-spark-runtime-3.3_2.12-0.7.2.jar"
    )

    @task
    def remove_files(date, **kwargs):
        resources_path_obj = pathlib.Path("/opt/spark/resources/data/")
        glob_pattern = "{}-*.json.gz".format(date)
        print("PATH GLOB STR: {}".format(glob_pattern))
        paths = (str(path) for path in sorted(resources_path_obj.glob(glob_pattern), key=os.path.getmtime))
        cur_path = next(paths, None)
        while cur_path is not None:
            print(cur_path)
            pathlib.unlink(cur_path)
            cur_path = next(paths, None)
        # return date.strftime("%F")

    [repos, commits, members] >> remove_files(date='{{ dag_run.conf.get("date") }}')