from util import *

def members_df(spark, df):
    df.createOrReplaceTempView("activity")

    return spark.sql(
        """
        SELECT
            date(created_at) as date,
            timestamp(created_at) as created_at,
            payload.member.login as member_login,
            payload.member.type as member_type,
            org.login as org_login,
            actor.login as actor_login,
            repo.name as repo_name_full,
            repo_name,
            repo_author
        FROM activity
        WHERE
            type = 'MemberEvent'
            and payload.action = 'added'
        """
    )

def create_clickhouse_members_table(table_name):
    create_table_cmd = """
    CREATE TABLE IF NOT EXISTS gharchive.{}
    (
        date Date,
        created_at DateTime,
        member_login String,
        member_type String,
        org_login Nullable(String),
        actor_login String,
        repo_name_full String,
        repo_name String,
        repo_author String
    )
    ENGINE MergeTree
    ORDER BY date;
    """.format(table_name)

    create_clickhouse_table(create_table_cmd)

def handle_metric_members(spark, df, table_name="members"):
    df = add_repo_columns(df)
    df_members = members_df(spark, df)

    ratio_compression(df_members, df)
    create_clickhouse_members_table(table_name)
    clickhouse_write(df_members, table_name)