import sys, os
import pathlib
# import pyspark
# import clickhouse_connect

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# from util import *
from repos import handle_metric_repos

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

def handle_file(path):
    df = spark.read.json(cur_path)
    print(df.count())
    handle_metric_repos(spark, df, table_name="repos")

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