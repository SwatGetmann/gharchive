import sys, os
import pathlib
import pyspark
import clickhouse_connect

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

##########################
# You can configure master here if you do not pass the spark.master paramenter in conf
##########################
#master = "spark://spark:7077"
#conf = SparkConf().setAppName("Spark Hello World").setMaster(master)
#sc = SparkContext(conf=conf)
# spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Create spark context
sc = SparkContext()
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()


def add_repo_columns(df):
    split_col = pyspark.sql.functions.split(df['repo.name'], '/')
    df = df.withColumn('repo_author', split_col.getItem(0))
    df = df.withColumn('repo_name', split_col.getItem(1))
    return df

def ratio_compression(df_new, df_old):
    print(
        """
        Degree of ~compression~ via aggregation: {:0.2f} ({} / {})
        """.format(
            df_old.count() / df_new.count(), 
            df_new.count(), 
            df_old.count()
        )
    )

def clickhouse_write(df, table_name):
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("user", "altenar") \
        .option("password", "altenar_ch_demo_517") \
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
        .option("url", "jdbc:clickhouse://clickhouse-server:9000/gharchive") \
        .option("dbtable", "gharchive.{}".format(table_name)) \
        .save()

def handle_metric_repo(df, table_name="repos"):
    df = add_repo_columns(df)
    df_repos = repo_df(df)

    ratio_compression(df_repos, df)
    create_clickhouse_repo_table(table_name)
    clickhouse_write(df_repos, table_name)

def handle_metric_commits(df, table_name="commits"):
    df_commits = commits_df(df)

    ratio_compression(df_commits, df)
    create_clickhouse_commits_table(table_name)
    clickhouse_write(df_commits, table_name)

def handle_metric_members(df, table_name="members"):
    df = add_repo_columns(df)
    df_members = members_df(df)

    ratio_compression(df_members, df)
    create_clickhouse_members_table(table_name)
    clickhouse_write(df_members, table_name)

def repo_df(df):
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

def commits_df(df):
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

def members_df(df):
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

def create_clickhouse_repo_table(table_name):
    client = clickhouse_connect.get_client(
        host='clickhouse-server', 
        username='altenar', 
        password='altenar_ch_demo_517'
    )

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

    print(create_table_cmd)
    client.command(create_table_cmd)
    client.close()

def create_clickhouse_commits_table(table_name):
    client = clickhouse_connect.get_client(
        host='clickhouse-server', 
        username='altenar', 
        password='altenar_ch_demo_517'
    )

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
    
    print(create_table_cmd)
    client.command(create_table_cmd)
    client.close()

def create_clickhouse_members_table(table_name):
    client = clickhouse_connect.get_client(
        host='clickhouse-server', 
        username='altenar', 
        password='altenar_ch_demo_517'
    )

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
    
    print(create_table_cmd)
    client.command(create_table_cmd)
    client.close()

def handle_file(path):
    df = spark.read.json(cur_path)
    print(df.count())

    handle_metric_repo(df, table_name="repos")
    handle_metric_commits(df, table_name="commits")
    handle_metric_members(df, table_name="members")


# Get the second argument passed to spark-submit (the first is the python app)
date = sys.argv[1]
resources_path_obj = pathlib.Path("/opt/spark/resources/data/")
glob_pattern = "{}-*.json.gz".format(date)
print("PATH GLOB STR: {}".format(glob_pattern))

paths = (str(path) for path in sorted(resources_path_obj.glob(glob_pattern), key=os.path.getmtime))
cur_path = next(paths, None)
while cur_path is not None:
    print(cur_path)
    handle_file(cur_path)
    cur_path = next(paths, None)

print("Processed all files for a given date!")