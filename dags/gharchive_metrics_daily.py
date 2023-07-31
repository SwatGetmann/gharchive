from airflow import DAG
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.decorators import task
from airflow.models.param import Param

from datetime import datetime, timedelta
import requests

import clickhouse_connect
import os

#### DAGs & Tasks

start_date = datetime(2023, 7, 26)

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
    dag_id="gharchive_03_metrics_daily",
    description="""
    Processor for GitHubArchive.
    - Generates CSV files for desired metrics.
    Note: uses clickhouse-connector.
    """,
    default_args=default_args, 
    schedule="@daily",
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['gharchive']
) as dag:

    def ch_stats(cmd):
        def ch_df(limit=0):
            client = clickhouse_connect.get_client(
                host='clickhouse_server', 
                username=os.environ['CLICKHOUSE_USER'], 
                password=os.environ['CLICKHOUSE_PASSWORD']
            )
            if limit > 0:
                df = client.query_df(query=cmd + " LIMIT {}".format(limit))
            else:
                df = client.query_df(query=cmd)
            
            client.close()
            return df
        return ch_df

    @task
    def get_repo_ownership_stats(**kwargs):
        repo_ownership_stats = ch_stats(
            cmd="""
            SELECT
                repo_author,
                countDistinct(repo_name) AS repos_total
            FROM gharchive.repos
            GROUP BY repo_author
            HAVING repos_total > 1
            ORDER BY repos_total DESC
            """
        )
        df = repo_ownership_stats()
        cur_date = kwargs['logical_date']
        df.to_csv("/opt/airflow/data/repos_{}.csv".format(cur_date.strftime("%F")))
    
    @task
    def get_commits_stats(**kwargs):
        commits_gt1_stats = ch_stats(
            cmd = """
            SELECT                                                                                                                                                                                                                                                                   
                date,
                author_name,
                count(*) AS total_commits
            FROM gharchive.commits
            GROUP BY
                date,
                author_name
            HAVING total_commits >= 2
            ORDER BY
                author_name ASC,
                total_commits DESC
            """
        )
        df_gt1 = commits_gt1_stats()
        cur_date = kwargs['logical_date']
        df_gt1.to_csv("/opt/airflow/data/commits_gt1_{}.csv".format(cur_date.strftime("%F")))

        commits_dts_stats = ch_stats(
            cmd = """
            SELECT
                toDate(min(created_at)) AS min_created_at,
                toDate(max(created_at)) AS max_created_at
            FROM gharchive.commits
            """
        )
        df_dates = commits_dts_stats()
        commits_lt1_stats = ch_stats(
            cmd = """
            SELECT t.author_name, t.interval_start_dt
            FROM
            (
                SELECT
                    author_name,
                    created_at,
                    1 AS total,
                    toStartOfInterval(created_at, toIntervalDay(1)) AS interval_start_dt,
                    sum(total) OVER (PARTITION BY author_name, interval_start_dt ORDER BY created_at ASC) AS sum_commits
                FROM gharchive.commits
                ORDER BY
                    author_name ASC,
                    interval_start_dt ASC 
                        WITH FILL 
                        FROM toUnixTimestamp('{}') 
                        TO toUnixTimestamp('{}') 
                        STEP toIntervalDay(1)
            ) AS t
            WHERE sum_commits = 0
            """.format(
                df_dates['min_created_at'][0].strftime("%F"),
                (df_dates['max_created_at'][0] + timedelta(days=1)).strftime("%F")
            )
        )
        df_lt1 = commits_lt1_stats()
        df_lt1.to_csv("/opt/airflow/data/commits_lt1_{}.csv".format(cur_date.strftime("%F")))

    @task
    def get_members_stats(**kwargs):
        members_gt10_stats = ch_stats(
            cmd = """
            SELECT
                repo_name,
                repo_name_full,
                countDistinct(member_login) AS total_members
            FROM gharchive.members
            GROUP BY
                repo_name,
                repo_name_full
            HAVING total_members > 10
            ORDER BY 
                total_members DESC,
                repo_name ASC
            """
        )
        df = members_gt10_stats()
        cur_date = kwargs['logical_date']
        df.to_csv("/opt/airflow/data/members_{}.csv".format(cur_date.strftime("%F")))

    get_repo_ownership_stats()
    get_commits_stats()
    get_members_stats()
