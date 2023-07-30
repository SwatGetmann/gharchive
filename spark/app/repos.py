from util import *

def repos_df(spark, df):
    df.createOrReplaceTempView("activity")

    return spark.sql(
        """
        SELECT
            date(created_at) as date,
            hour(created_at) as hour,
            repo_author, 
            repo_name,
            count(*) as total_events
        FROM activity 
        GROUP BY 
            date,
            hour,
            repo_author,
            repo_name
        ORDER BY total_events DESC
        """
    )

def create_clickhouse_repos_table(table_name):
    create_table_cmd = """
    CREATE TABLE IF NOT EXISTS gharchive.{}
    (
        date Date,
        hour UInt8,
        repo_author String,
        repo_name String,
        total_events UInt32
    )
    ENGINE MergeTree
    ORDER BY date;
    """.format(table_name)

    create_clickhouse_table(create_table_cmd)

def handle_metric_repos(spark, df, table_name="repos"):
    df = add_repo_columns(df)
    df_repos = repos_df(spark, df)

    ratio_compression(df_repos, df)
    create_clickhouse_repos_table(table_name)
    clickhouse_write(df_repos, table_name)