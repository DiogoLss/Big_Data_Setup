from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Session") \
    .config("spark.driver.host","172.18.0.7")\
    .getOrCreate()


df = spark.read.format('csv').load('s3a://raw/nubank/fatura/')
df.show()