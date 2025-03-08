from pyspark.sql import SparkSession
from delta import *

class Spark:
    def __init__(self):
        self.spark = configure_spark_with_delta_pip(
            SparkSession.builder
            .appName("Spark_Application")
            .enableHiveSupport()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            ).getOrCreate()
