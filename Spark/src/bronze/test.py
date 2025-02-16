from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from pyspark.sql import SparkSession

# Criação de uma SparkSession com configuração para usar o Hive
spark = SparkSession.builder \
    .appName("Test Hive Metastore") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

spark.sql("SHOW TABLES").show()