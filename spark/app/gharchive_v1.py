import sys, os
import pathlib
import pyspark
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


def prepare_first_metric(df):
    df.createOrReplaceTempView("activity")

    df_repos = spark.sql(
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

    print(
    """
    Degree of ~compression~ via aggregation: {:0.2f} ({} / {})
    """.format(df.count() / df_repos.count(), df_repos.count(), df.count())
    )

    return df_repos


def prepare_clickhouse_first_metric(table_name):
    import clickhouse_connect

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


def handle_file(path):
    df = spark.read.json(cur_path)
    print(df.count())
    df = add_repo_columns(df)
    df_repos = prepare_first_metric(df)
    
    table_name = "repo_aggregated"
    prepare_clickhouse_first_metric(table_name)
    
    df_repos.write \
        .format("jdbc") \
        .mode("append") \
        .option("user", "altenar") \
        .option("password", "altenar_ch_demo_517") \
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
        .option("url", "jdbc:clickhouse://clickhouse-server:9000/gharchive") \
        .option("dbtable", "gharchive.{}".format(table_name)) \
        .save()


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