import sys, os
import pathlib
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

# Get the second argument passed to spark-submit (the first is the python app)
date = sys.argv[1]
resources_path_obj = pathlib.Path("/opt/spark/resources/data/")
path_glob_str = "{}-*.json.gz".format(date)
print("PATH GLOB STR: {}".format(path_glob_str))

paths = [str(path) for path in sorted(resources_path_obj.glob(path_glob_str), key=os.path.getmtime)]
print(paths)

df = spark.read.json(paths)
print(df.show(10))

print(df.count())
