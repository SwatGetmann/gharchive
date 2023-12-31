import pyspark
import clickhouse_connect
import os

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
        .option("user", os.environ['CLICKHOUSE_USER']) \
        .option("password", os.environ['CLICKHOUSE_PASSWORD']) \
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
        .option("url", "jdbc:clickhouse://clickhouse-server:9000/gharchive") \
        .option("dbtable", "gharchive.{}".format(table_name)) \
        .save()

def create_clickhouse_table(create_table_cmd):
    client = clickhouse_connect.get_client(
        host='clickhouse-server', 
        username=os.environ['CLICKHOUSE_USER'], 
        password=os.environ['CLICKHOUSE_PASSWORD']
    )

    print(create_table_cmd)
    client.command(create_table_cmd)
    client.close()