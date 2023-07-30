from util import *

def commits_df(spark, df):
    df.createOrReplaceTempView("activity")

    return spark.sql(
        """
        SELECT
            date(created_at) as date,
            timestamp(created_at) as created_at,
            explode(payload.commits.author.name) as author_name
        FROM activity
        WHERE 
            type = 'PushEvent'
        """
    )

def create_clickhouse_commits_table(table_name):
    create_table_cmd = """
    CREATE TABLE IF NOT EXISTS gharchive.{}
    (
        date Date,
        created_at DateTime,
        author_name String
    )
    ENGINE MergeTree
    ORDER BY date;
    """.format(table_name)

    create_clickhouse_table(create_table_cmd)

def handle_metric_commits(spark, df, table_name="commits"):
    df_commits = commits_df(spark, df)

    ratio_compression(df_commits, df)
    create_clickhouse_commits_table(table_name)
    clickhouse_write(df_commits, table_name)