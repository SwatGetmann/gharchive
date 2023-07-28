import sys
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
file_path = sys.argv[1]
print(file_path)



spark.read.json(file_path)
print(sc.show(10))
