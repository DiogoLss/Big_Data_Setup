from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Session") \
    .getOrCreate()


df = spark.read.format('parquet').option('header','true').load('s3a://data/bronze/nubank/extrato/')
df.show()