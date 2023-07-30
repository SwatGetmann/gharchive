from airflow import DAG
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.decorators import task
from airflow.models.param import Param

from datetime import datetime, timedelta
import requests


#### DAGs & Tasks

start_date = datetime(2023, 7, 20)

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
    dag_id="gharchive_01_crawl_daily",
    description="""
    Processor for GitHubArchive.
    - Crawls the archive for the past day (and even catches up), or for selected day
    - Stores the file in shared Spark resource folder
    # - Passes the file paths to Spark for aggregations
    # - Runs Spark job with the crawled file
    # - Saves the result from Spark into Clcikhouse
    # - Removes the file after completion
    """,
    default_args=default_args, 
    # schedule_interval=timedelta(hours=1),
    schedule="@daily",
    # catchup=True,
    catchup=False,
    concurrency=4,
    max_active_runs=1,
    params={
        "start_date": Param(
            f"{start_date}",
            type="string",
            format="datetime",
            description="Define the datetime, to process the data for",
            title="Datetime to proocess"
        )
    },
    tags=['gharchive']
) as dag:

    @task
    def get_date(**kwargs):
        dag_run = kwargs['dag_run']
        if 'start_date' not in dag_run.conf:
            print("Start date had not been given!")
            print("Using the logical date and the previous one from today!")
            date = kwargs['logical_date']    
        else:
            date = datetime.strptime(dag_run.conf['start_date'], "%Y-%m-%d")
        
        print("GIVEN DT: {}".format(date))
        prev_dt = date - timedelta(days=1)
        prev_dt = prev_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        print("PREVIOUS DAY DT: {}".format(prev_dt))
        return prev_dt

    @task
    def generate_dts(date, **kwargs):
        dts = [date.replace(hour=i) for i in range(0, 24)]
        return dts

    @task
    def gharchive_path(dt, **kwargs):
        print("GIVEN DT: {}".format(dt))

        path = "{:04d}-{:02d}-{:02d}-{}.json.gz".format(
            dt.year, 
            dt.month, 
            dt.day, 
            dt.hour
        )

        return path

    @task
    def crawl_gharchive(path, **kwargs):
        print("PATH: {}".format(path))
        archive_url = "https://data.gharchive.org/{}".format(path)
        save_path = "/opt/spark/resources/data/{}".format(path)
        print("ARCHIVE URL: {}".format(archive_url))
        print("SAVE PATH: {}".format(save_path))
        resp = requests.get(archive_url)
        if resp.status_code == 200:
            with open(save_path , "wb") as f:
                f.write(resp.content)
        return save_path

    @task
    def dummy_merger(date, crawled_paths, **kwargs):
        return date.strftime("%F")

    date = get_date()
    dts = generate_dts(date=date)
    paths = gharchive_path.expand(dt=dts)
    crawled_paths = crawl_gharchive.expand(path=paths)

    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        trigger_dag_id="gharchive_02_spark",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"date": dummy_merger(date, crawled_paths)},
    )
