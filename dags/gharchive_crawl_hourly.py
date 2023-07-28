from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

# from airflow.decorators import dag, task

from datetime import datetime, timedelta
# import dateutil.parser
import pendulum

import requests

####
# TODO:
# - Crawl __past__ hour of gharchive for current day (or past one)
# - 
####

now = datetime.now()

#### FUNCTIONALITY

def gharchive_path(dag_run, **kwargs):
    print("GIVEN DT: {}".format(dag_run.logical_date))
    dt_hour_earlier = dag_run.logical_date - timedelta(hours=1)
    print("EARLIER DT (-1h): {}".format(dt_hour_earlier))

    path = "{:04d}-{:02d}-{:02d}-{}.json.gz".format(
        dt_hour_earlier.year, 
        dt_hour_earlier.month, 
        dt_hour_earlier.day, 
        dt_hour_earlier.hour
    )

    task_instance = kwargs['ti']
    task_instance.xcom_push(key='path', value=path)

def crawl_gharchive(**kwargs):
    # print(kwargs)
    # path = gharchive_path(kwargs['logical_date'])
    
    task_instance = kwargs['ti']
    path = task_instance.xcom_pull(key='path', task_ids='make_gharchive_path')

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


#### DAGs & Tasks

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="gharchive_hourly",
    description="""
    Processor for GitHubArchive.
    - Crawls the archive for logical_date, past hour
    - Stores the file in shared Spark resource folder
    """,
    default_args=default_args, 
    # schedule_interval=timedelta(hours=1),
    schedule="@hourly",
    # catchup=False
)

start = DummyOperator(task_id="start", dag=dag)

date = '{{ ts }}'

task_make_gharchive_path = PythonOperator(
    task_id='make_gharchive_path',
    python_callable=gharchive_path,
    provide_context=True,
    dag=dag,
)

task_crawl_gharchive = PythonOperator(
    task_id='crawl_gharchive',
    python_callable=crawl_gharchive,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> task_make_gharchive_path >> task_crawl_gharchive >> end